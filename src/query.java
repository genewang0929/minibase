import bufmgr.BufMgr;
import diskmgr.PCounter;
import global.*;
import heap.Heapfile;
import heap.Tuple;
import iterator.*;
import lshfindex.*;

import java.io.*;
import java.util.Arrays;

public class query {

  public static void main(String[] args) {
    // Expecting: query DBNAME QSNAME INDEXOPTION NUMBUF
    if (args.length != 4) {
      System.err.println("Usage: query DBNAME QSNAME INDEXOPTION NUMBUF");
      System.exit(1);
    }

    PCounter.initialize();

    String dbName = args[0];
    String qsName = args[1];
    String indexOption = args[2];
    int numBuf = Integer.parseInt(args[3]);

    // Initialize the buffer manager with NUMBUF pages.
    BufMgr bufMgr = new BufMgr(numBuf, null);

    try {
      QuerySpec qs = parseQuerySpec(qsName);
      Vector100Dtype targetVector = readTargetVector(qs.getTargetFileName());

      String dbpath = "/tmp/"+System.getProperty("user.name")+"."+dbName;
      SystemDefs.MINIBASE_RESTART_FLAG = true;  // Use the existing DBMS
      SystemDefs sysdef = new SystemDefs(dbpath, 1000, numBuf, "Clock" );
      Heapfile heapFile = new Heapfile("batch_file");

      AttrType[] attrTypes = null;
      short[] Ssizes = new short[1];
      Ssizes[0] = 30;
      attrTypes = get_attrTypes(dbName, attrTypes);

      FldSpec[] projlist = new FldSpec[qs.getOutputFields().length];
      RelSpec rel = new RelSpec(RelSpec.outer);
      for (int i = 0; i < qs.getOutputFields().length; i++)
        projlist[i] = new FldSpec(rel, i + 1);

      // Get output fields attributes types
      AttrType[] outAttrTypes = new AttrType[qs.getOutputFields().length];
      for (int i = 0; i < qs.getOutputFields().length; i++) {
        outAttrTypes[i] = attrTypes[qs.getOutputFields()[i] - 1];
      }


      // TODO: LSH Query
      if (indexOption.equalsIgnoreCase("Y")) {
        String indexFileName = "batch_index_file";
        LSHFIndexFile lshf = new LSHFIndexFile(indexFileName);
        // Heapfile hf = new Heapfile("batch_file");
        LSHFFileScan scan = new LSHFFileScan(lshf, heapFile, targetVector);
        String keyStr = lshf.computeHash(targetVector, 0, lshf.getH());
        Vector100DKey key = new Vector100DKey(keyStr);
        if (qs.getQueryType() == QueryType.RANGE) {
          
          // IntegerKey convertedKey = LSHF.convertKey(key);
          Tuple[] results = scan.LSHFFileRangeScan(key, qs.getThreshold(), attrTypes, qs.getQueryField());
          
          if (results != null && results.length != 0) {
            System.out.println("Range Search Test: " + results[0]);
          }
        } else if (qs.getQueryType() == QueryType.NN) {
          Tuple[] results = scan.LSHFFileNNScan(key, qs.getThreshold(), attrTypes, qs.getQueryField());
          
          if (results != null && results.length != 0) {
            System.out.println("Range Search Test: " + results[0]);
          }
        }
      }
      else {
        // Full scan without using any index.

        if (qs.getQueryType() == QueryType.RANGE) {
          // **File Scan with Range Condition**
          CondExpr[] rangeCond = new CondExpr[2]; // For single condition, CondExpr[2] is usually enough, last one is null
          rangeCond[0] = new CondExpr();
          rangeCond[0].op = new AttrOperator(AttrOperator.aopLE); // Less than or equal to range
          rangeCond[0].type1 = new AttrType(AttrType.attrSymbol);
          rangeCond[0].type2 = new AttrType(AttrType.attrVector100D);
          rangeCond[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), qs.getQueryField());
          rangeCond[0].operand2.vector100D = targetVector;
          rangeCond[0].distance = qs.getThreshold(); // Set target vector for distance calculation
          rangeCond[1] = null; // Terminator


          FileScan fileScan = new FileScan("batch_file", attrTypes, Ssizes, (short)attrTypes.length,
                  qs.getOutputFields().length, projlist, rangeCond); // Apply condition during scan

          Tuple resultTuple;
          while ((resultTuple = fileScan.get_next()) != null) {
            resultTuple.print(attrTypes);
          }
          fileScan.close();
        }
        else if (qs.getQueryType() == QueryType.NN) {
          // **File Scan and Sort for NN (using Sort iterator)**
          TupleOrder[] order = new TupleOrder[2];
          order[0] = new TupleOrder(TupleOrder.Ascending);
          order[1] = new TupleOrder(TupleOrder.Descending);

          // print outputAttrTypes
//          for (int i = 0; i < outAttrTypes.length; i++) {
//            System.out.println(outAttrTypes[i].attrType);
//          }

          FileScan fileScan = new FileScan("batch_file", attrTypes, Ssizes, (short)attrTypes.length, outAttrTypes, qs.getOutputFields().length, projlist, null);

          Sort sortIterator = new Sort(outAttrTypes, (short) outAttrTypes.length, Ssizes,
                  fileScan, qs.getQueryField(), order[0], 32, 500, targetVector, qs.getThreshold());

          Tuple resultTuple;
          System.out.println("Result Tuple:");
          while ((resultTuple = sortIterator.get_next()) != null) {
            resultTuple.print(outAttrTypes);
          }
          System.exit(1);
          sortIterator.close();
          fileScan.close();
        }

      }

      // --- Step 4: Report Disk I/O Statistics ---
      System.out.println("Page reads: " + PCounter.rcounter);
      System.out.println("Page writes: " + PCounter.wcounter);

    } catch (Exception e) {
      System.err.println("Error executing query: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /** Reads the schema file and returns an array of AttrType objects
   * representing the attribute types for the specified database.
   * The schema file is expected to be in the format:
   *    <number_of_attributes>
   *    <type1> <type2> ... <typeN>
   * where each type is represented by an integer code:
   * 1 - Integer, 2 - Real, 3 - String, 4 - Vector100D
   */
  public static AttrType[] get_attrTypes(String dbName, AttrType[] attrTypes) {
    try (BufferedReader schemaReader = new BufferedReader(new FileReader( "batch_file.schema"))) {
      String line = schemaReader.readLine();
      if (line == null) { throw new IOException("Schema file is empty or corrupted."); }
      int numAttributes = Integer.parseInt(line.trim());
      attrTypes = new AttrType[numAttributes];

      line = schemaReader.readLine();
      if (line == null) { throw new IOException("Schema file is incomplete."); }
      String[] typeStrings = line.trim().split("\\s+");
      if (typeStrings.length != numAttributes) {
        throw new IOException("Schema file attribute type count mismatch.");
      }

      for (int i = 0; i < numAttributes; i++) {
        int typeCode = Integer.parseInt(typeStrings[i]);
        switch (typeCode) {
          case 1: attrTypes[i] = new AttrType(AttrType.attrInteger); break;
          case 2: attrTypes[i] = new AttrType(AttrType.attrReal); break;
          case 3: attrTypes[i] = new AttrType(AttrType.attrString); break;
          case 4: attrTypes[i] = new AttrType(AttrType.attrVector100D); break;
          default: throw new IOException("Unknown attribute type code in schema file: " + typeCode);
        }
      }
      System.out.println("Attribute types read from schema file.");

    } catch (IOException e) {
      System.err.println("Error reading schema file: " + e.getMessage());
      System.exit(1); // Or handle error appropriately
    }
    return attrTypes;
  }

  /** Parses the query specification file and returns a QuerySpec object
   * containing the parsed information. The query specification file is expected
   * to be in the format:
   *    Range(<queryField>, <targetFileName>, <threshold>, <outputField1>, <outputField2>, ...)
   * or
   *    NN(<queryField>, <targetFileName>, <K>, <outputField1>, <outputField2>, ...)
   */
  private static QuerySpec parseQuerySpec(String qsName) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(qsName));
    String line = br.readLine().trim();
    br.close();

    QuerySpec qs = new QuerySpec();

    if (line.startsWith("Range(")) {
      qs.setQueryType(QueryType.RANGE);
      String inside = line.substring("Range(".length(), line.length() - 1);
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs.setQueryField(Integer.parseInt(tokens[0]));     // QA
      qs.setTargetFileName(tokens[1]);                     // T: target vector file name
      qs.setThreshold(Integer.parseInt(tokens[2]));        // D: distance threshold
      int numOut = tokens.length - 3;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 3]);
      }
      qs.setOutputFields(outFields);
    }
    else if (line.startsWith("NN(")) {
      qs.setQueryType(QueryType.NN);
      String inside = line.substring("NN(".length(), line.length() - 1);
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs.setQueryField(Integer.parseInt(tokens[0]));     // QA
      qs.setTargetFileName(tokens[1]);                     // T: target vector file name
      qs.setThreshold(Integer.parseInt(tokens[2]));        // K: number of nearest neighbors
      int numOut = tokens.length - 3;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 3]);
      }
      qs.setOutputFields(outFields);
    }
    else {
      throw new IllegalArgumentException("Invalid query specification format: " + line);
    }

    return qs;
  }

  /** Reads the target vector from the specified file and returns it as a Vector100Dtype object.
   * The target vector file is expected to contain 100 integers, one for each dimension of the vector.
   */
  private static Vector100Dtype readTargetVector(String fileName) throws IOException {
    if (!fileName.endsWith(".txt"))
      fileName += ".txt";
    BufferedReader br = new BufferedReader(new FileReader("queries/" + fileName));
    String line = br.readLine().trim();
    br.close();
    String[] tokens = line.split("\\s+");
    if (tokens.length != 100) {
      throw new IllegalArgumentException("Target vector file must contain 100 integers.");
    }
    short[] vector = new short[100];
    for (int i = 0; i < 100; i++) {
      vector[i] = Short.parseShort(tokens[i]);
    }
    return new Vector100Dtype(vector);
  }

}


