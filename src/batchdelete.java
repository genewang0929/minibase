import btree.*;
import dbmgr.DBOP;
import diskmgr.PCounter;
import global.AttrType;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lshfindex.*;

public class batchdelete {

  private static boolean DEBUG = true;

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: batchdelete <UPDATEFILENAME> >RELNAME>");
      System.exit(1);
    }

    PCounter.initialize();

    String updateFilename = args[0];
    String relName = args[1];

    DBOP.open_databaseDBNAME("mydb", 500, 1000);

    try {
      BufferedReader br = new BufferedReader(new FileReader(updateFilename));
      int numAttrs = Integer.parseInt(br.readLine().trim());
      String[] typeTokens = br.readLine().trim().split("\\s+");

      AttrType[] attrTypes = new AttrType[numAttrs];
      Map<Integer, AttrType> attrTypeMap = new HashMap<>(); // Map fildNo to AttrType, e.g. For input 2, 4, 2, 4 -> map = {1:2, 2:4, 3:2, 4:4}
      for (int i = 0; i < numAttrs; i++) {
        int typeCode = Integer.parseInt(typeTokens[i]);
        switch (typeCode) {
        case 1:
          attrTypes[i] = new AttrType(AttrType.attrInteger);
          attrTypeMap.put(i + 1, new AttrType(AttrType.attrInteger));
          break;
        case 2:
          attrTypes[i] = new AttrType(AttrType.attrReal);
          attrTypeMap.put(i + 1, new AttrType(AttrType.attrReal));
          break;
        case 3:
          attrTypes[i] = new AttrType(AttrType.attrString);
          attrTypeMap.put(i + 1, new AttrType(AttrType.attrString));
          break;
        case 4:
          attrTypes[i] = new AttrType(AttrType.attrVector100D);
          attrTypeMap.put(i + 1, new AttrType(AttrType.attrVector100D));
          break;
        default:
          throw new IOException("Unknown attribute type code: " + typeCode);
        }
      }

      System.out.println("attrTypeMap: " + attrTypeMap);

      // Open an exising heap file for storing tuples (database file)
      Heapfile hf = new Heapfile(relName);
      System.out.println("Heapfile contains " + hf.getRecCnt() + " tuples before deletion.");
      Scan scan = null;

      // Process tuples.
      // Each tuple is stored in the file as n consecutive lines.
      String line;
      boolean eof = false;
      while ((line = br.readLine()) != null) {
        int attr_num = Integer.parseInt(line.trim().split(" ")[0]);

        try {
          scan = hf.openScan();
          RID rid = new RID();

          Tuple tuple = new Tuple();
          boolean done = false;
          boolean is_deleted = false;

          while (!done && !is_deleted) {
            try {
              tuple = scan.getNext(rid);
//              System.out.printf("tuple with RID<%d, %d>\n", rid.pageNo.pid, rid.slotNo);
              if (tuple == null) {
                done = true;
              }
            } catch (Exception e) {
              System.err.println("*** Error getting next tuple\n");
              e.printStackTrace();
            }

            if (!done && !is_deleted) {
              tuple.setHdr((short) numAttrs, attrTypes, null);
              // Check if the tuple matches the attr_num to be deleted
              switch (attrTypeMap.get(attr_num).attrType) {
              case AttrType.attrInteger: {
                int intValue = tuple.getIntFld(attr_num);
//                  System.out.println("intValue: " + intValue);

                // Check if the intValue matches the attribute to be deleted
                int deleteValue = Integer.parseInt(line.trim().split(" ")[1]);

                // update index files
                updateIndexFiles(relName, attrTypes, tuple, rid);
                // try {
                //   BTreeFile btree_int = new BTreeFile(relName);
                //   KeyClass intKey = new IntegerKey(deleteValue);
                //   btree_int.Delete(intKey, rid);
                //   btree_int.close();
                // } catch (Exception e) {
                //   System.out.println("Index File for this column does not exist");
                // }

                if (intValue == deleteValue) {
                  // Delete the record
                  try {
                    is_deleted = hf.deleteRecord(rid);
                    System.out.println("integer deleted? " + is_deleted);
                  } catch (Exception e) {
                    System.err.println("*** Error deleting record");
                    e.printStackTrace();
                    break;
                  }
                }
                break;
              }
              case AttrType.attrReal: {
                float floatValue = tuple.getFloFld(attr_num);
//                  System.out.println("floatValue: " + floatValue);

                // Check if the floatValue matches the attribute to be deleted
                float deleteFloatValue = Float.parseFloat(line.trim().split(" ")[1]);

                // update index files
                updateIndexFiles(relName, attrTypes, tuple, rid);
                // try {
                //   BTreeFile btree_real = new BTreeFile(relName);
                //   KeyClass realKey = new IntegerKey((int)deleteFloatValue);
                //   btree_real.Delete(realKey, rid);
                //   btree_real.close();
                // } catch (Exception e) {
                //   System.out.println("Index File for this column does not exist");
                // }

                if (floatValue == deleteFloatValue) {
                  // Delete the record
                  System.out.printf("Delete tuple with RID<%d, %d>\n", rid.pageNo.pid, rid.slotNo);
                  try {
                    is_deleted = hf.deleteRecord(rid);
                    System.out.println("real deleted? " + is_deleted);
                  } catch (Exception e) {
                    System.err.println("*** Error deleting record");
                    e.printStackTrace();
                    break;
                  }
                }
                break;
              }
              case AttrType.attrString: {
                String strValue = tuple.getStrFld(attr_num);
//                  System.out.println("strValue: " + strValue);

                // Check if the strValue matches the attribute to be deleted
                String[] deletedStr = line.trim().split(" ");
                String[] deletedStrValues = new String[deletedStr.length - 1];
                for (int i = 1; i < deletedStr.length; i++) {
                  deletedStrValues[i - 1] = deletedStr[i];
                }
                String deleteStrValue = String.join(" ", deletedStrValues);

                // update index files
                updateIndexFiles(relName, attrTypes, tuple, rid);
                // try {
                //   BTreeFile btree_str = new BTreeFile(relName);
                //   KeyClass strKey = new StringKey(deleteStrValue);
                //   btree_str.Delete(strKey, rid);
                //   btree_str.close();
                // } catch (Exception e) {
                //   System.out.println("Index File for this column does not exist");
                // }

                if (strValue.equals(deleteStrValue)) {
                  // Delete the record
                  try {
                    is_deleted = hf.deleteRecord(rid);
                    System.out.println("string deleted? " + is_deleted);
                  } catch (Exception e) {
                    System.err.println("*** Error deleting record");
                    e.printStackTrace();
                    break;
                  }
                }
                break;
              }
              case AttrType.attrVector100D: {
                Vector100Dtype vector100Dtype = tuple.get100DVectFld(attr_num);
//                  System.out.println("vector100Dtype");
//                  for (int i = 0; i < vector100Dtype.getDimension().length; i++) {
//                    System.out.print(vector100Dtype.getDimension()[i] + " ");
//                  }
//                  System.out.println();

                // Check if the vector100Dtype matches the attribute to be deleted
                Vector100Dtype deleteVector100Dtype = new Vector100Dtype();
                String[] deletedVectorValuesStr = line.trim().split(" ");
                short[] deletedVectorValues = new short[deletedVectorValuesStr.length - 1];
                for (int i = 1; i < deletedVectorValuesStr.length; i++) {
                  deletedVectorValues[i - 1] = Short.parseShort(deletedVectorValuesStr[i]);
                }
                deleteVector100Dtype.setDimension(deletedVectorValues);

                // update index files
                updateIndexFiles(relName, attrTypes, tuple, rid);
                // try {
                //   LSHFIndexFile lshf = new LSHFIndexFile(relName);
                //   lshf.delete(deleteVector100Dtype, rid);
                // } catch (Exception e) {
                //   System.out.println("Index File for this column does not exist");
                // }

                if (vector100Dtype.equals(deleteVector100Dtype)) {
                  // Delete the record
                  try {
                    is_deleted = hf.deleteRecord(rid);
                    System.out.println("vector deleted? " + is_deleted);
                  } catch (Exception e) {
                    System.err.println("*** Error deleting record");
                    e.printStackTrace();
                    break;
                  }
                }
                break;
              }
              default: { System.err.println("Unknown attribute number: " + attr_num); }
              }
            }
          }

          scan.closescan();  //  destruct scan!!!!!!!!!!!!!!!
        } catch (Exception e) {
          System.err.println("*** Error opening scan\n");
          e.printStackTrace();
        }
      }

      SystemDefs.JavabaseBM.flushAllPages();

      System.out.println("Heapfile contains " + hf.getRecCnt() + " tuples after deletion.");
      // At the end of batch insertion, output the disk I/O counts.
      System.out.println("Page reads: " + PCounter.rcounter);
      System.out.println("Page writes: " + PCounter.wcounter);
    } catch (Exception e) {
      System.err.println("Error reading update file: " + e.getMessage());
      System.exit(1);
    }
  }

  // update index files
  private static void updateIndexFiles(String relName, AttrType[] attrTypes, Tuple tuple, RID rid) {
    // update index files
    if (DEBUG) {
      System.out.println("==========================================");
      System.out.println("Updating index files for record (" + rid.pageNo.pid + ", " + rid.slotNo + ")");
    }

    for (int i = 0; i < attrTypes.length; i++) {
      switch (attrTypes[i].attrType) {
      case AttrType.attrInteger: {
        // Check if the intValue matches the attribute to be inserted
        try {
          int intValue = tuple.getIntFld(i + 1);

          // update index files
          try {
            BTreeFile btree_int = new BTreeFile(relName + "_" + (i+1));
            System.out.println("Index File successfully opened");
            KeyClass intKey = new IntegerKey(intValue);
            System.out.println("Key successfully created");
            btree_int.Delete(intKey, rid);
            System.out.println("Index File successfully updated");
            btree_int.close();
            System.out.println("Index File successfully closed");
          } catch (Exception e) {
            System.out.println("Index File for this column does not exist: " + relName + "_" + (i+1));
          }

        } catch (Exception e) {
          System.err.println("Get int field failed");
        }

        break;
      }
      case AttrType.attrReal: {
        // Check if the floatValue matches the attribute to be inserted
        try {
          float floatValue = tuple.getFloFld(i + 1);

          // update index files
          try {
            BTreeFile btree_real = new BTreeFile(relName + "_" + (i+1));
            System.out.println("Index File successfully opened");
            KeyClass realKey = new IntegerKey((int)floatValue);
            System.out.println("Key successfully created");
            btree_real.Delete(realKey, rid);
            System.out.println("Index File successfully updated");
            btree_real.close();
            System.out.println("Index File successfully closed");
          } catch (Exception e) {
            System.out.println("Index File for this column does not exist: " + relName + "_" + (i+1));
          }

        }  catch (Exception e) {
          System.err.println("Get float field failed");
        }

        break;
      }
      case AttrType.attrString: {
        // Check if the strValue matches the attribute to be inserted
        try {
          String strValue = tuple.getStrFld(i + 1);

          // update index files
          try {
            BTreeFile btree_str = new BTreeFile(relName + "_" + (i+1));
            System.out.println("Index File successfully opened");
            KeyClass strKey = new StringKey(strValue);
            System.out.println("Key successfully created");
            btree_str.Delete(strKey, rid);
            System.out.println("Index File successfully updated");
            btree_str.close();
            System.out.println("Index File successfully closed");
          } catch (Exception e) {
            System.out.println("Index File for this column does not exist: " + relName + "_" + (i+1));
          }

        }  catch (Exception e) {
          System.err.println("Get string field failed");
        }

        break;
      }
      case AttrType.attrVector100D: {
        try {
          Vector100Dtype vector100Dtype = tuple.get100DVectFld(i + 1);

          // update index files
          try {
            LSHFIndexFile lshf = new LSHFIndexFile(relName + "_" + (i + 1));
            System.out.println("Index File successfully opened");
            lshf.insert(vector100Dtype, rid);
            System.out.println("Index File successfully updated");
            lshf.close();
            System.out.println("Index File successfully closed");
          } catch (Exception e) {
            System.out.println("Index File for this column does not exist: " + relName + "_" + (i+1));
          }

        }  catch (Exception e) {
          System.err.println("Get vector field failed");
        }

        break;
      }
      default: { System.err.println("Unknown attribute number: " + i); }
      }
    }
  }

}