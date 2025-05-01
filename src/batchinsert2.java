import dbmgr.DBOP;
import diskmgr.PCounter;
import global.AttrType;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.Heapfile;
import heap.Tuple;
import btree.*;
import lshfindex.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static dbmgr.DBOP.*;

public class batchinsert2 {

  private static boolean DEBUG = true;

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: batchinsert <UPDATEFILENAME> >RELNAME>");
      System.exit(1);
    }

    PCounter.initialize();

    String updateFilename = args[0];
    String relName = args[1];

    SystemDefs.MINIBASE_RESTART_FLAG = true;  // Use the existing DBMS
    DBOP.open_databaseDBNAME("mydb", 500, 1000);

    try {
      BufferedReader br = new BufferedReader(new FileReader(updateFilename));
      int numAttrs = Integer.parseInt(br.readLine().trim());
      String[] typeTokens = br.readLine().trim().split("\\s+");

      AttrType[] attrTypes = new AttrType[numAttrs];

      if (DEBUG) {
        System.out.println("attrTypes.length: " + attrTypes.length);
      }

      for (int i = 0; i < numAttrs; i++) {
        int typeCode = Integer.parseInt(typeTokens[i]);
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
          throw new IOException("Unknown attribute type code: " + typeCode);
        }
      }


      // Open an exising heap file for storing tuples (database file)
      Heapfile hf = new Heapfile(relName);

      // Process tuples.
      // Each tuple is stored in the file as n consecutive lines.
      String line;
      boolean eof = false;
      while ((line = br.readLine()) != null) {
        // Read one tuple; first attribute is already in 'line'
        String[] tupleValues = new String[numAttrs];
        tupleValues[0] = line;
        for (int i = 1; i < numAttrs; i++) {
          String nextLine = br.readLine();
          if (nextLine != null) {
            System.out.println("nextLine: " + nextLine);
//            String truncated = nextLine.substring(0, 50);
//            System.out.println("nextLine: " + truncated + "...");
          }
          if (nextLine == null) {
            eof = true;
            System.err.println("End of file.");
            break;
          }
          tupleValues[i] = nextLine;
        }
        if (eof) {
          break;
        }

        // Create a tuple with numAttrs fields.
        Tuple tuple = create_tuple(numAttrs, attrTypes, tupleValues);

        // Convert the tuple to a byte array and insert it into the heap file.
        byte[] tupleData = tuple.getTupleByteArray();
        RID rid = hf.insertRecord(tupleData);
        System.out.printf("Inserted tuple with RID<%d, %d>\n", rid.pageNo.pid, rid.slotNo);

        // update index files
        for (int i = 0; i < attrTypes.length; i++) {
          switch (attrTypes[i].attrType) {
            case AttrType.attrInteger: {
              // Check if the intValue matches the attribute to be inserted
              int insertValue = tuple.getIntFld(i+1);

              // update index files
              try {
                BTreeFile btree_int = new BTreeFile(relName + "_" + (i+1));
                KeyClass intKey = new IntegerKey(insertValue);
                btree_int.insert(intKey, rid);
                btree_int.close();
                System.out.println("Index File successfully updated");
              } catch (Exception e) {
                System.out.println("Index File for this column does not exist");
              }

              break;
            }
            case AttrType.attrReal: {
              // Check if the floatValue matches the attribute to be inserted
              float insertFloatValue = tuple.getFloFld(i+1);

              // update index files
              try {
                BTreeFile btree_real = new BTreeFile(relName + "_" + (i+1));
                KeyClass realKey = new IntegerKey((int)insertFloatValue);
                btree_real.insert(realKey, rid);
                btree_real.close();
                System.out.println("Index File successfully updated");
              } catch (Exception e) {
                System.out.println("Index File for this column does not exist");
              }

              break;
            }
            case AttrType.attrString: {
              // Check if the strValue matches the attribute to be inserted
              String insertStrValue = tuple.getStrFld(i+1);

              // update index files
              try {
                BTreeFile btree_str = new BTreeFile(relName + "_" + (i+1));
                KeyClass strKey = new StringKey(insertStrValue);
                btree_str.insert(strKey, rid);
                btree_str.close();
                System.out.println("Index File successfully updated");
              } catch (Exception e) {
                System.out.println("Index File for this column does not exist");
              }

              break;
            }
            case AttrType.attrVector100D: {
              Vector100Dtype vector100Dtype = tuple.get100DVectFld(i+1);

              // update index files
              try {
                LSHFIndexFile lshf = new LSHFIndexFile(relName + "_" + (i+1));
                lshf.insert(vector100Dtype, rid);
                System.out.println("Index File successfully updated");
                lshf.close();
                System.out.println("Index File successfully closed");
              } catch (Exception e) {
                System.out.println("Index File for this column does not exist");
              }

              break;
            }
            default: { System.err.println("Unknown attribute number: " + i); }
          }
        }

      }
      DBOP.close_database();

      // SystemDefs.JavabaseBM.flushAllPages();

      System.out.println("Heapfile contains " + hf.getRecCnt() + " tuples");
      // At the end of batch insertion, output the disk I/O counts.
      System.out.println("Page reads: " + PCounter.rcounter);
      System.out.println("Page writes: " + PCounter.wcounter);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
