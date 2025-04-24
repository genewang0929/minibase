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
      QuerySpec[] qs_list = parseQuerySpec(qsName);
      QuerySpec qs = qs_list[0];
      QuerySpec qs2 = qs_list[1];

      Vector100Dtype targetVector = readTargetVector(
              qs.getTargetFileName()
      );

      Heapfile heapFile1 = new Heapfile(relName1);
      Heapfile heapFile2 = new Heapfile(relName2);

      AttrType[] attrTypes1 = null;
      AttrType[] attrTypes2 = null;
      short[] Ssizes = new short[1];
      Ssizes[0] = 30;
      short[] Rsizes = new short[1];
      Rsizes[0] = 15;

      String attrTypeFile1 = "./schemas/" + relName1 + ".schema";
      String attrTypeFile2 = "./schemas/" + relName2 + ".schema";
      attrTypes1 = get_attrTypes(attrTypeFile1, attrTypes1);
      attrTypes2 = get_attrTypes(attrTypeFile2, attrTypes2);

      FldSpec[] projlist = new FldSpec[qs.getOutputFields().length];
      RelSpec rel = new RelSpec(RelSpec.outer);
      for (int i = 0; i < qs.getOutputFields().length; i++)
        projlist[i] =
                new FldSpec(rel, i + 1);

      // Get output fields attributes types
      AttrType[] outAttrTypes = new AttrType[qs.getOutputFields().length];
      for (int i = 0; i < qs.getOutputFields().length; i++) {
        outAttrTypes[i] = attrTypes1[qs.getOutputFields()[i] - 1];
      }

      if (qs2.getQueryType() != null) {
        // DJOIN operation

        // project of the first relation
        FldSpec[] proj_rel1 = new FldSpec[attrTypes1.length];
        for (int i = 0; i < attrTypes1.length; i++) {
          proj_rel1[i] = new FldSpec(
                  new RelSpec(RelSpec.outer),
                  i + 1
          );
        }

        // project of the joined output relation (first + second)
        FldSpec[] proj_join = new FldSpec[qs.getOutputFields().length + qs2.getOutputFields().length];
        for (int i = 0; i < qs.getOutputFields().length; i++) {
          proj_join[i] = new FldSpec(
                  new RelSpec(RelSpec.outer),
                  qs.getOutputFields()[i]
          );
        }
        for (int i = 0; i < qs2.getOutputFields().length; i++) {
          proj_join[i + qs.getOutputFields().length] = new FldSpec(
                  new RelSpec(RelSpec.innerRel),
                  qs2.getOutputFields()[i]
          );
        }

        AttrType[] joinAttrTypes = new AttrType[qs.getOutputFields().length + qs2.getOutputFields().length];
        for (int i = 0; i < qs.getOutputFields().length; i++) {
          joinAttrTypes[i] = attrTypes1[qs.getOutputFields()[i] - 1];
        }
        for (int i = 0; i < qs2.getOutputFields().length; i++) {
          joinAttrTypes[i + qs.getOutputFields().length] = attrTypes2[qs2.getOutputFields()[i] - 1];
        }

        if (qs2.getQueryType() == QueryType.RANGE) {
          // Range operation
          System.out.println("Performing DJOIN with RANGE operation...");
          if (qs2.getUseIndex()) {
            System.out.println("Using index for DJOIN query...");
          }
          else {
            // Two FileScans: 1. Range query on first relation 2. Range query on second relation
            CondExpr[] outFilter1 = new CondExpr[2];
            outFilter1[0] = new CondExpr();
            outFilter1[0].next = null;
            outFilter1[0].op = new AttrOperator(AttrOperator.aopLE);
            outFilter1[0].type1 = new AttrType(AttrType.attrSymbol);
            outFilter1[0].type2 = new AttrType(AttrType.attrVector100D);
            outFilter1[0].operand1.symbol = new FldSpec(
                    new RelSpec(RelSpec.outer),
                    qs.getQueryField()
            );
            outFilter1[0].operand2.vector100D = targetVector;
            outFilter1[0].distance = qs.getThreshold();
            outFilter1[1] = null;

            CondExpr[] outFilter2 = new CondExpr[2];
            outFilter2[0] = new CondExpr();
            outFilter2[0].next = null;
            outFilter2[0].op = new AttrOperator(AttrOperator.aopLE);
            outFilter2[0].type1 = new AttrType(AttrType.attrSymbol);
            outFilter2[0].type2 = new AttrType(AttrType.attrSymbol);
            outFilter2[0].operand1.symbol = new FldSpec(  // the first relation is outer
                    new RelSpec(RelSpec.outer),
                    qs.getQueryField()
            );
            outFilter2[0].operand2.symbol = new FldSpec(  // the second relation is inner
                    new RelSpec(RelSpec.innerRel),
                    qs2.getQueryField()
            );
            outFilter2[0].distance = qs2.getThreshold();
            outFilter2[1] = null;

            FileScan am = null;
            try {
              am = new FileScan(
                      relName1,
                      attrTypes1,
                      Ssizes,
                      (short) attrTypes1.length,
                      attrTypes1,
                      attrTypes1.length,
                      proj_rel1,
                      outFilter1
              ); // Apply condition during scan
            } catch (Exception e) {
              e.printStackTrace();
            }

            NestedLoopsJoins inl = null;
            try {
              inl = new NestedLoopsJoins(attrTypes1, attrTypes1.length, Ssizes,
                      attrTypes2, attrTypes2.length, Rsizes,
                      10,
                      am, relName2,
                      outFilter2, null, proj_join, joinAttrTypes.length);
            } catch (Exception e) {
              System.err.println("*** Error preparing for nested_loop_join");
              System.err.println("" + e);
              e.printStackTrace();
              Runtime.getRuntime().exit(1);
            }

            Tuple resultTuple;
            System.out.println("Result Tuple:");
            while ((resultTuple = inl.get_next()) != null) {
              resultTuple.print(joinAttrTypes);
            }
            inl.close();
            am.close();
          }
        }
        else if (qs2.getQueryType() == QueryType.NN) {
          // Nearest Neighbor operation
          System.out.println("Performing DJOIN with NN operation...");
          if (qs2.getUseIndex()) {
            System.out.println("Using index for DJOIN query...");
          } else {
            System.out.println("Not using index for DJOIN query...");
            CondExpr[] outFilter = new CondExpr[2];
            outFilter[0] = new CondExpr();
            outFilter[0].next = null;
            outFilter[0].op = new AttrOperator(AttrOperator.aopLE);
            outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
            outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
            outFilter[0].operand1.symbol = new FldSpec(  // the first relation is outer
                    new RelSpec(RelSpec.outer),
                    qs.getQueryField()
            );
            outFilter[0].operand2.symbol = new FldSpec(  // the second relation is inner
                    new RelSpec(RelSpec.innerRel),
                    qs2.getQueryField()
            );
            outFilter[0].distance = qs2.getThreshold();
            outFilter[1] = null;

            TupleOrder[] order = new TupleOrder[2];
            order[0] = new TupleOrder(TupleOrder.Ascending);
            order[1] = new TupleOrder(TupleOrder.Descending);

            FileScan am = new FileScan(
                    relName1,
                    attrTypes1,
                    Ssizes,
                    (short) attrTypes1.length,
                    attrTypes1,
                    attrTypes1.length,
                    proj_rel1,
                    null
            );

            Sort sortIterator = new Sort(
                    attrTypes1,
                    (short) attrTypes1.length,
                    Ssizes,
                    am,
                    qs.getQueryField(),
                    order[0],
                    32,
                    500,
                    targetVector,
                    qs.getThreshold()
            );

            NestedLoopsJoins inl = null;
            try {
              inl = new NestedLoopsJoins(attrTypes1, attrTypes1.length, Ssizes,
                      attrTypes2, attrTypes2.length, Rsizes,
                      10,
                      sortIterator, relName2,
                      outFilter, null, proj_join, joinAttrTypes.length);
            } catch (Exception e) {
              System.err.println("*** Error preparing for nested_loop_join");
              System.err.println("" + e);
              e.printStackTrace();
              Runtime.getRuntime().exit(1);
            }

            Tuple resultTuple;
            System.out.println("Result Tuple:");
            while ((resultTuple = inl.get_next()) != null) {
              resultTuple.print(joinAttrTypes);
            }
            inl.close();
            sortIterator.close();
            am.close();
          }
        }
      }
      else if (qs.getQueryType() == QueryType.SORT) {
        // Sort operation
        FileScan fileScan = new FileScan(
                relName1,
                attrTypes1,
                Ssizes,
                (short) attrTypes1.length,
                outAttrTypes,
                qs.getOutputFields().length,
                projlist,
                null
        );

        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);
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
      else if (qs.getQueryType() == QueryType.FILTER) {
        // Filter operation
        System.out.println("Performing Filter operation...");

        if (qs.getUseIndex()) {
          System.out.println("Using index for filter query...");
        }
        else {
          // **File Scan with Range Condition**
          CondExpr[] rangeCond = new CondExpr[2]; // For single condition, CondExpr[2] is usually enough, last one is null
          rangeCond[0] = new CondExpr();
          rangeCond[0].op = new AttrOperator(AttrOperator.aopEQ); // Less than or equal to range
          rangeCond[0].type1 = new AttrType(AttrType.attrSymbol);
          rangeCond[0].type2 = new AttrType(AttrType.attrVector100D);
          rangeCond[0].operand1.symbol = new FldSpec(
                  new RelSpec(RelSpec.outer),
                  qs.getQueryField()
          );
          rangeCond[0].operand2.vector100D = targetVector;
          rangeCond[0].distance = 0;
          rangeCond[1] = null; // Terminator

          FileScan fileScan = new FileScan(
                  relName1,
                  attrTypes1,
                  Ssizes,
                  (short) attrTypes1.length,
                  outAttrTypes,
                  qs.getOutputFields().length,
                  projlist,
                  rangeCond
          ); // Apply condition during scan

          Tuple resultTuple;
          while ((resultTuple = fileScan.get_next()) != null) {
            resultTuple.print(attrTypes1);
          }
          fileScan.close();
        }
      }
      else if (qs.getQueryType() == QueryType.RANGE) {
        // Range operation
        System.out.println("Performing Range operation...");

        if (qs.getUseIndex()) {
          System.out.println("Using index for range query...");
        }
        else {
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
                  attrTypes1,
                  Ssizes,
                  (short) attrTypes1.length,
                  outAttrTypes,
                  qs.getOutputFields().length,
                  projlist,
                  rangeCond
          ); // Apply condition during scan

          Tuple resultTuple;
          while ((resultTuple = fileScan.get_next()) != null) {
            resultTuple.print(attrTypes1);
          }
          fileScan.close();
        }
      }
      else if (qs.getQueryType() == QueryType.NN) {
        // Nearest Neighbor operation
        if (qs.getUseIndex()) {
          System.out.println("Using index for NN query...");
        } else {
          System.out.println("Using file scan for NN query...");
          // **File Scan and Sort for NN (using Sort iterator)**
          TupleOrder[] order = new TupleOrder[2];
          order[0] = new TupleOrder(TupleOrder.Ascending);
          order[1] = new TupleOrder(TupleOrder.Descending);

          FileScan fileScan = new FileScan(
                  relName1,
                  attrTypes1,
                  Ssizes,
                  (short) attrTypes1.length,
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

      System.out.println("Page reads: " + PCounter.rcounter);
      System.out.println("Page writes: " + PCounter.wcounter);
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
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

  private static QuerySpec[] parseQuerySpec(String qsName) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(qsName));
    String line = br.readLine().trim();

    QuerySpec[] qs = new QuerySpec[2];
    qs[0] = new QuerySpec();
    qs[1] = new QuerySpec();

    if (line.startsWith("Sort(")) {
      qs[0].setQueryType(QueryType.SORT);
      String inside = line.substring("Sort(".length(), line.length() - 1);
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
      qs[0].setTargetFileName(tokens[1]); // T: target vector file name
      qs[0].setThreshold(Integer.parseInt(tokens[2])); // K
      int numOut = tokens.length - 3;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 3]);
      }
      qs[0].setOutputFields(outFields);
    }
    else if (line.startsWith("Filter(")) {
      qs[0].setQueryType(QueryType.FILTER);
      String inside = line.substring(
              "Filter(".length(),
              line.length() - 1
      );
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
      qs[0].setTargetFileName(tokens[1]); // T: target vector file name
      qs[0].setUseIndex(tokens[2] == "Y"); // Use index or not
      int numOut = tokens.length - 3;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 3]);
      }
      qs[0].setOutputFields(outFields);
    }
    else if (line.startsWith("Range(")) {
      qs[0].setQueryType(QueryType.RANGE);
      String inside = line.substring(
              "Range(".length(),
              line.length() - 1
      );
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
      qs[0].setTargetFileName(tokens[1]); // T: target vector file name
      qs[0].setThreshold(Integer.parseInt(tokens[2])); // D: distance threshold
      qs[0].setUseIndex(tokens[3] == "Y"); // Use index or not
      int numOut = tokens.length - 4;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 4]);
      }
      qs[0].setOutputFields(outFields);
    }
    else if (line.startsWith("NN(")) {
      qs[0].setQueryType(QueryType.NN);
      String inside = line.substring("NN(".length(), line.length() - 1);
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
      qs[0].setTargetFileName(tokens[1]); // T: target vector file name
      qs[0].setThreshold(Integer.parseInt(tokens[2])); // K: number of nearest neighbors
      qs[0].setUseIndex(tokens[3] == "Y"); // Use index or not
      int numOut = tokens.length - 4;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 4]);
      }
      qs[0].setOutputFields(outFields);
    }
    else if (line.startsWith("DJOIN(")) {
      // We have 2 more lines to read
      line = br.readLine().trim();
      if (line.startsWith("Range(")) {
        // First relation
        qs[0].setQueryType(QueryType.RANGE);
        String inside = line.substring("Range(".length(), line.length() - 2);
        String[] tokens = inside.split(",");
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = tokens[i].trim();
        }
        qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
        qs[0].setTargetFileName(tokens[1]); // T: target vector file name
        qs[0].setThreshold(Integer.parseInt(tokens[2])); // D: distance threshold
        qs[0].setUseIndex(tokens[3] == "Y"); // Use index or not
        int numOut = tokens.length - 4;
        int[] outFields = new int[numOut];
        for (int i = 0; i < numOut; i++) {
          outFields[i] = Integer.parseInt(tokens[i + 4]);
        }
        qs[0].setOutputFields(outFields);

        // Second relation
        line = br.readLine().trim();
        qs[1].setQueryType(QueryType.RANGE);
        inside = line.substring(0, line.length() - 2);
        tokens = inside.split(",");
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = tokens[i].trim();
        }
        qs[1].setQueryField(Integer.parseInt(tokens[0])); // QA
        qs[1].setThreshold(Integer.parseInt(tokens[1])); // D: distance threshold
        qs[1].setUseIndex(tokens[2] == "Y"); // Use index or not
        numOut = tokens.length - 3;
        outFields = new int[numOut];
        for (int i = 0; i < numOut; i++) {
          outFields[i] = Integer.parseInt(tokens[i + 3]);
        }
        qs[1].setOutputFields(outFields);

      }
      else if (line.startsWith("NN(")) {
        qs[0].setQueryType(QueryType.NN);
        String inside = line.substring("NN(".length(), line.length() - 2);
        String[] tokens = inside.split(",");
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = tokens[i].trim();
        }
        qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
        qs[0].setTargetFileName(tokens[1]); // T: target vector file name
        qs[0].setThreshold(Integer.parseInt(tokens[2])); // K: number of nearest neighbors
        qs[0].setUseIndex(tokens[3] == "Y"); // Use index or not
        int numOut = tokens.length - 4;
        int[] outFields = new int[numOut];
        for (int i = 0; i < numOut; i++) {
          outFields[i] = Integer.parseInt(tokens[i + 4]);
        }
        qs[0].setOutputFields(outFields);

        // Second relation
        line = br.readLine().trim();
        qs[1].setQueryType(QueryType.NN);
        inside = line.substring(0, line.length() - 2);
        tokens = inside.split(",");
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = tokens[i].trim();
        }
        qs[1].setQueryField(Integer.parseInt(tokens[0])); // QA
        qs[1].setThreshold(Integer.parseInt(tokens[1])); // K: number of nearest neighbors
        qs[1].setUseIndex(tokens[2] == "Y"); // Use index or not
        numOut = tokens.length - 3;
        outFields = new int[numOut];
        for (int i = 0; i < numOut; i++) {
          outFields[i] = Integer.parseInt(tokens[i + 3]);
        }
        qs[1].setOutputFields(outFields);

      }
      else {
        throw new IllegalArgumentException(
                "Invalid query specification format: " + line
        );
      }
    } else {
      throw new IllegalArgumentException(
              "Invalid query specification format: " + line
      );
    }
    br.readLine();

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
