package dbmgr;

import global.AttrType;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;
import java.io.PrintWriter;

public class DBOP {

  public static void open_databaseDBNAME(String dbName, int numPages, int numBuf) {
    String dbpath = "./dbinstance/" + dbName;
    // Initialize DB
    SystemDefs sysdef = new SystemDefs(dbpath, numPages, numBuf, "Clock");
  }

  public static void close_database() {
    // Close the database
    try {
      SystemDefs.JavabaseBM.flushAllPages();
    } catch (Exception e) {
      System.err.println("Error closing database: " + e);
    }
  }

  public static void cleanup(String dbName) {

    String dbpath = "/tmp/" + System.getProperty("user.name") + "." + dbName;
    String logpath = "/tmp/" + System.getProperty("user.name") + ".log";

    String remove_cmd = "/bin/rm -rf ";
    String remove_logcmd = remove_cmd + logpath;
    String remove_dbcmd = remove_cmd + dbpath;
    try {
      Runtime.getRuntime().exec(remove_logcmd);
      Runtime.getRuntime().exec(remove_dbcmd);
    } catch (IOException e) {
      System.err.println("" + e);
    }
  }

  public static Tuple create_tuple(int numAttrs, AttrType[] attrTypes, String[] tupleValues)
          throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException {
    Tuple tuple = new Tuple();
    short[] Ssizes = new short[1];
    Ssizes[0] = 30;
    tuple.setHdr((short) numAttrs, attrTypes, Ssizes);
    for (int i = 0; i < numAttrs; i++) {
      switch (attrTypes[i].attrType) {
        case AttrType.attrInteger:
          int intVal = Integer.parseInt(tupleValues[i].trim());
          tuple.setIntFld(i + 1, intVal);
          break;
        case AttrType.attrReal:
          float floatVal = Float.parseFloat(tupleValues[i].trim());
          tuple.setFloFld(i + 1, floatVal);
//          System.out.println("floatVal: " + tuple.getFloFld(i + 1));
          break;
        case AttrType.attrString:
          String strVal = tupleValues[i].trim();
          tuple.setStrFld(i + 1, strVal);
          break;
        case AttrType.attrVector100D:
          // Expect the line to contain 100 numbers separated by whitespace.
          String[] vecTokens = tupleValues[i].trim().split("\\s+");
          if (vecTokens.length != 100) {
            System.err.println("Error: Expected 100 numbers for 100D-vector, found " + vecTokens.length);
            System.exit(1);
          }
          short[] dims = new short[100];
          for (int j = 0; j < 100; j++) {
            float temp = Float.parseFloat(vecTokens[j].trim());
            dims[j] = (short) temp;
          }
          Vector100Dtype vector = new Vector100Dtype(dims);
          tuple.set100DVectFld(i + 1, vector);
//          System.out.println("vector");
//          for (int k = 0; k < vector.getDimension().length; k++) {
//            System.out.print(vector.getDimension()[k] + " ");
//          }
          System.out.println();
          break;
        default:
          System.err.println("Unknown attribute type: " + attrTypes[i]);
      }
    }
    return tuple;
  }



  public static void save_attrTypes(String heapFilename, int numAttrs, AttrType[] attrTypes) {
    String schemaFileName = "./schemas/" + heapFilename + ".schema";
    try (PrintWriter schemaWriter = new PrintWriter(schemaFileName)) {
      schemaWriter.println(numAttrs);
      for (int i = 0; i < numAttrs; i++) {
        int typeCode = 0;
        if (attrTypes[i].attrType == AttrType.attrInteger) typeCode = 1;
        else if (attrTypes[i].attrType == AttrType.attrReal) typeCode = 2;
        else if (attrTypes[i].attrType == AttrType.attrString) typeCode = 3;
        else if (attrTypes[i].attrType == AttrType.attrVector100D) typeCode = 4;
        schemaWriter.print(typeCode + (i == numAttrs - 1 ? "" : " "));
      }
      schemaWriter.println();
      System.out.println("Schema information saved to " + heapFilename + ".schema");
    } catch (IOException e) {
      System.err.println("Error writing schema file: " + e.getMessage());
      System.exit(1);
    }
  }
}
