import bufmgr.BufMgr;
import dbmgr.DBOP;
import diskmgr.PCounter;
import global.*;
import heap.Heapfile;
import heap.Tuple;
import iterator.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class query2 {

  public static void main(String[] args) {
    // Expecting: query DBNAME QSNAME INDEXOPTION NUMBUF
    if (args.length != 4) {
      System.err.println("Usage: query RELNAME1 RELNAME2 QSNAME NUMBUF");
      System.exit(1);
    }

    PCounter.initialize();
    String relName1 = args[0];
    String relName2 = args[1];
    String qsName = args[2];
    int numBuf = Integer.parseInt(args[3]);

    SystemDefs.MINIBASE_RESTART_FLAG = true; // Use the existing DBMS
    DBOP.open_databaseDBNAME("mydb", 1000, numBuf);

    // Initialize the buffer manager with NUMBUF pages.
    BufMgr bufMgr = new BufMgr(numBuf, null);

    try {
      QuerySpec qs = parseQuerySpec(qsName);
      Vector100Dtype targetVector = readTargetVector(
              qs.getTargetFileName()
      );

      Heapfile heapFile = new Heapfile(relName1);

      AttrType[] attrTypes = null;
      short[] Ssizes = new short[1];
      Ssizes[0] = 30;
      String attrTypeFile = "./schemas/" + relName1 + ".schema";
      attrTypes = get_attrTypes(attrTypeFile, attrTypes);

      FldSpec[] projlist = new FldSpec[qs.getOutputFields().length];
      RelSpec rel = new RelSpec(RelSpec.outer);
      for (int i = 0; i < qs.getOutputFields().length; i++)
        projlist[i] =
                new FldSpec(rel, i + 1);

      // Get output fields attributes types
      AttrType[] outAttrTypes = new AttrType[qs.getOutputFields().length];
      for (int i = 0; i < qs.getOutputFields().length; i++) {
        outAttrTypes[i] = attrTypes[qs.getOutputFields()[i] - 1];
      }

      if (qs.getQueryType() == QueryType.SORT) {
        // Sort operation
        sort_query(
                relName1,
                attrTypes,
                outAttrTypes,
                Ssizes,
                targetVector,
                qs,
                projlist
        );
      } else if (qs.getQueryType() == QueryType.FILTER) {
        // Filter operation
        System.out.println("Performing Filter operation...");
        // Implement filtering logic here
      } else if (qs.getQueryType() == QueryType.RANGE) {
        // Range operation
        range_query(
                relName1,
                attrTypes,
                outAttrTypes,
                Ssizes,
                qs,
                projlist,
                targetVector
        );
      } else if (qs.getQueryType() == QueryType.NN) {
        // Nearest Neighbor operation
        nn_query(
                relName1,
                attrTypes,
                outAttrTypes,
                Ssizes,
                targetVector,
                qs,
                projlist
        );
      } else {
        throw new IllegalArgumentException(
                "Unknown query type: " + qs.getQueryType()
        );
      }

      System.out.println("Page reads: " + PCounter.rcounter);
      System.out.println("Page writes: " + PCounter.wcounter);
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private static void sort_query(
          String relName1,
          AttrType[] attrTypes,
          AttrType[] outAttrTypes,
          short[] Ssizes,
          Vector100Dtype targetVector,
          QuerySpec qs,
          FldSpec[] projlist
  ) throws Exception {
    System.out.println("Performing Sort operation...");
    TupleOrder[] order = new TupleOrder[2];
    order[0] = new TupleOrder(TupleOrder.Ascending);
    order[1] = new TupleOrder(TupleOrder.Descending);

    // print outputAttrTypes
    //    for (int i = 0; i < outAttrTypes.length; i++) {
    //      System.out.println(outAttrTypes[i]);
    //    }

    FileScan fileScan = new FileScan(
            relName1,
            attrTypes,
            Ssizes,
            (short) attrTypes.length,
            outAttrTypes,
            qs.getOutputFields().length,
            projlist,
            null
    );

    Sort sortIterator = new Sort(
            outAttrTypes,
            (short) outAttrTypes.length,
            Ssizes,
            fileScan,
            qs.getQueryField(),
            order[0],
            32,
            500,
            targetVector,
            qs.getThreshold()
    );

    Tuple resultTuple;
    System.out.println("Result Tuple:");
    while ((resultTuple = sortIterator.get_next()) != null) {
      resultTuple.print(outAttrTypes);
    }
    sortIterator.close();
    fileScan.close();
  }

  private static void range_query(
          String relName1,
          AttrType[] attrTypes,
          AttrType[] outAttrTypes,
          short[] Ssizes,
          QuerySpec qs,
          FldSpec[] projlist,
          Vector100Dtype targetVector
  ) throws Exception {
    System.out.println("Performing Range operation...");

    if (qs.getUseIndex()) {
      // Implement index-based range query here
      System.out.println("Using index for range query...");
    } else {
      System.out.println("Not using index for range query...");

      // **File Scan with Range Condition**
      CondExpr[] rangeCond = new CondExpr[2]; // For single condition, CondExpr[2] is usually enough, last one is null
      rangeCond[0] = new CondExpr();
      rangeCond[0].op = new AttrOperator(AttrOperator.aopLE); // Less than or equal to range
      rangeCond[0].type1 = new AttrType(AttrType.attrSymbol);
      rangeCond[0].type2 = new AttrType(AttrType.attrVector100D);
      rangeCond[0].operand1.symbol = new FldSpec(
              new RelSpec(RelSpec.outer),
              qs.getQueryField()
      );
      rangeCond[0].operand2.vector100D = targetVector;
      rangeCond[0].distance = qs.getThreshold(); // Set target vector for distance calculation
      rangeCond[1] = null; // Terminator

      FileScan fileScan = new FileScan(
              relName1,
              attrTypes,
              Ssizes,
              (short) attrTypes.length,
              outAttrTypes,
              qs.getOutputFields().length,
              projlist,
              rangeCond
      ); // Apply condition during scan

      Tuple resultTuple;
      while ((resultTuple = fileScan.get_next()) != null) {
        resultTuple.print(attrTypes);
      }
      fileScan.close();
    }
  }

  private static void nn_query(
          String relName1,
          AttrType[] attrTypes,
          AttrType[] outAttrTypes,
          short[] Ssizes,
          Vector100Dtype targetVector,
          QuerySpec qs,
          FldSpec[] projlist
  ) throws Exception {
    if (qs.getUseIndex()) {
      System.out.println("Using index for NN query...");
    } else {
      System.out.println("Using file scan for NN query...");
      // **File Scan and Sort for NN (using Sort iterator)**
      TupleOrder[] order = new TupleOrder[2];
      order[0] = new TupleOrder(TupleOrder.Ascending);
      order[1] = new TupleOrder(TupleOrder.Descending);

      // print outputAttrTypes
      //    for (int i = 0; i < outAttrTypes.length; i++) {
      //      System.out.println(outAttrTypes[i].attrType);
      //    }

      FileScan fileScan = new FileScan(
              relName1,
              attrTypes,
              Ssizes,
              (short) attrTypes.length,
              outAttrTypes,
              qs.getOutputFields().length,
              projlist,
              null
      );

      Sort sortIterator = new Sort(
              outAttrTypes,
              (short) outAttrTypes.length,
              Ssizes,
              fileScan,
              qs.getQueryField(),
              order[0],
              32,
              500,
              targetVector,
              qs.getThreshold()
      );

      Tuple resultTuple;
      System.out.println("Result Tuple:");
      while ((resultTuple = sortIterator.get_next()) != null) {
        resultTuple.print(outAttrTypes);
      }
      sortIterator.close();
      fileScan.close();
    }
  }

  public static AttrType[] get_attrTypes(
          String attrTypeFile,
          AttrType[] attrTypes
  ) {
    try (
            BufferedReader schemaReader = new BufferedReader(
                    new FileReader(attrTypeFile)
            )
    ) {
      String line = schemaReader.readLine();
      if (line == null) {
        throw new IOException("Schema file is empty or corrupted.");
      }
      int numAttributes = Integer.parseInt(line.trim());
      attrTypes = new AttrType[numAttributes];

      line = schemaReader.readLine();
      if (line == null) {
        throw new IOException("Schema file is incomplete.");
      }
      String[] typeStrings = line.trim().split("\\s+");
      if (typeStrings.length != numAttributes) {
        throw new IOException(
                "Schema file attribute type count mismatch."
        );
      }

      for (int i = 0; i < numAttributes; i++) {
        int typeCode = Integer.parseInt(typeStrings[i]);
        switch (typeCode) {
          case 1:
            attrTypes[i] = new AttrType(AttrType.attrInteger);
            break;
          case 2:
            attrTypes[i] = new AttrType(AttrType.attrReal);
            break;
          case 3:
            attrTypes[i] = new AttrType(AttrType.attrString);
            break;
          case 4:
            attrTypes[i] = new AttrType(AttrType.attrVector100D);
            break;
          default:
            throw new IOException(
                    "Unknown attribute type code in schema file: " +
                            typeCode
            );
        }
      }
      System.out.println("Attribute types read from schema file.");
    } catch (IOException e) {
      System.err.println("Error reading schema file: " + e.getMessage());
      System.exit(1); // Or handle error appropriately
    }
    return attrTypes;
  }

  private static QuerySpec parseQuerySpec(String qsName) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(qsName));
    String line = br.readLine().trim();
    br.close();

    QuerySpec qs = new QuerySpec();

    if (line.startsWith("Sort(")) {
      qs.setQueryType(QueryType.SORT);
      String inside = line.substring("Sort(".length(), line.length() - 1);
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs.setQueryField(Integer.parseInt(tokens[0])); // QA
      qs.setTargetFileName(tokens[1]); // T: target vector file name
      qs.setThreshold(Integer.parseInt(tokens[2])); // K
      int numOut = tokens.length - 3;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 3]);
      }
      qs.setOutputFields(outFields);
    } else if (line.startsWith("Filter(")) {
      qs.setQueryType(QueryType.FILTER);
      String inside = line.substring(
              "Filter(".length(),
              line.length() - 1
      );
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs.setQueryField(Integer.parseInt(tokens[0])); // QA
      qs.setTargetFileName(tokens[1]); // T: target vector file name
      qs.setThreshold(Integer.parseInt(tokens[2])); // D: distance threshold
    } else if (line.startsWith("Range(")) {
      qs.setQueryType(QueryType.RANGE);
      String inside = line.substring(
              "Range(".length(),
              line.length() - 1
      );
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs.setQueryField(Integer.parseInt(tokens[0])); // QA
      qs.setTargetFileName(tokens[1]); // T: target vector file name
      qs.setThreshold(Integer.parseInt(tokens[2])); // D: distance threshold
      qs.setUseIndex(tokens[3] == "Y"); // Use index or not
      int numOut = tokens.length - 4;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 4]);
      }
      qs.setOutputFields(outFields);
    } else if (line.startsWith("NN(")) {
      qs.setQueryType(QueryType.NN);
      String inside = line.substring("NN(".length(), line.length() - 1);
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs.setQueryField(Integer.parseInt(tokens[0])); // QA
      qs.setTargetFileName(tokens[1]); // T: target vector file name
      qs.setThreshold(Integer.parseInt(tokens[2])); // K: number of nearest neighbors
      qs.setUseIndex(tokens[3] == "Y"); // Use index or not
      int numOut = tokens.length - 4;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 4]);
      }
      qs.setOutputFields(outFields);
    } else {
      throw new IllegalArgumentException(
              "Invalid query specification format: " + line
      );
    }

    return qs;
  }

  private static Vector100Dtype readTargetVector(String fileName)
          throws IOException {
    if (!fileName.endsWith(".txt")) fileName += ".txt";
    BufferedReader br = new BufferedReader(
            new FileReader("datafiles/phase3/" + fileName)
    );
    String line = br.readLine().trim();
    br.close();
    String[] tokens = line.split("\\s+");
    if (tokens.length != 100) {
      throw new IllegalArgumentException(
              "Target vector file must contain 100 integers."
      );
    }
    short[] vector = new short[100];
    for (int i = 0; i < 100; i++) {
      vector[i] = Short.parseShort(tokens[i]);
    }
    return new Vector100Dtype(vector);
  }
}

/**
 * Enum to represent the type of query being executed.
 * RANGE: Range query based on a distance threshold.
 * NN: Nearest Neighbor query to find K nearest neighbors.
 */
enum QueryType {
  SORT,
  FILTER,
  RANGE,
  NN,
}

/**
 * Class to encapsulate the query specification.
 * It contains fields for the query type, query field, target vector file name,
 * threshold (distance or number of neighbors), and output fields.
 */

class QuerySpec {

  private QueryType queryType;
  private int queryField; // Field number for the 100D-vector attribute.
  private String targetFileName; // Name of the file containing the target vector.
  private int threshold; // For Range: the distance D; for NN: the number K.
  private boolean useIndex; // Whether to use index or not.
  private int[] outputFields; // Field numbers to output.

  public QueryType getQueryType() {
    return queryType;
  }

  public void setQueryType(QueryType queryType) {
    this.queryType = queryType;
  }

  public int getQueryField() {
    return queryField;
  }

  public void setQueryField(int queryField) {
    this.queryField = queryField;
  }

  public String getTargetFileName() {
    return targetFileName;
  }

  public void setTargetFileName(String targetFileName) {
    this.targetFileName = targetFileName;
  }

  public int getThreshold() {
    return threshold;
  }

  public void setThreshold(int threshold) {
    this.threshold = threshold;
  }

  public void setUseIndex(boolean useIndex) {
    this.useIndex = useIndex;
  }

  public boolean getUseIndex() {
    return useIndex;
  }

  public int[] getOutputFields() {
    return outputFields;
  }

  public void setOutputFields(int[] outputFields) {
    this.outputFields = outputFields;
  }
}
