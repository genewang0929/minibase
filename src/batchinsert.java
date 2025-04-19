import dbmgr.DBOP;
import diskmgr.*;
import global.*;

import static dbmgr.DBOP.*;
import static global.GlobalConst.NUMBUF;
import heap.*;
import java.io.*;
import lshfindex.LSHFIndexFile;

public class batchinsert {

    private static boolean DEBUG = true;

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: batchinsert <h> <L> <DATAFILENAME> <DBNAME>");
            System.exit(1);
        }

        PCounter.initialize();

        try {
            System.out.println("Welcome!");
            int h = Integer.parseInt(args[0]);
            int L = Integer.parseInt(args[1]);
            String dataFilename = args[2];
            String dbName = args[3];
            String heapFilename = "batch_heap_file";

            // Clean up before any operation
            cleanup("mydb");

            BufferedReader br = new BufferedReader(new FileReader(dataFilename));
            int numAttrs = Integer.parseInt(br.readLine().trim());
            String[] typeTokens = br.readLine().trim().split("\\s+");

            AttrType[] attrTypes = new AttrType[numAttrs];
            for (int i = 0; i < numAttrs; i++) {
                int typeCode = Integer.parseInt(typeTokens[i]);
                switch (typeCode) {
                case 1: attrTypes[i] = new AttrType(AttrType.attrInteger); break;
                case 2: attrTypes[i] = new AttrType(AttrType.attrReal); break;
                case 3: attrTypes[i] = new AttrType(AttrType.attrString); break;
                case 4: attrTypes[i] = new AttrType(AttrType.attrVector100D); break;
                default: throw new IOException("Unknown attribute type code: " + typeCode);
                }
            }

            save_attrTypes(heapFilename, numAttrs, attrTypes);

            // Initialize DB
            DBOP.open_databaseDBNAME("mydb", NUMBUF, 1000);

            // Create the heap file for storing tuples (database file)
            Heapfile hf = new Heapfile(heapFilename);

            System.out.println("Now try to create LSHFIndexFile:");

            // Create exactly one LSHFIndexFile for all 100D-vector attributes.
            // This index file will store all the 100D vectors from every tuple.
            String LSHFIndexFileName = "batch_index_file";
            LSHFIndexFile vectorIndex = new LSHFIndexFile(LSHFIndexFileName, h, L);

            System.out.println("LSHFIndexFile created.");

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
                        String truncated = nextLine.substring(0, 50);
                        System.out.println("nextLine: " + truncated + "...");
                    }
                    if (nextLine == null) {
                        eof = true;
                        System.err.println("End of file.");
                        break;
                    }
                    tupleValues[i] = nextLine;
                }
                if (eof)
                    break;

                // Create a tuple with numAttrs fields.
                Tuple tuple = create_tuple(numAttrs, attrTypes, tupleValues);

                // Convert the tuple to a byte array and insert it into the heap file.
                byte[] tupleData = tuple.getTupleByteArray();
                RID rid = hf.insertRecord(tupleData);
                System.out.printf("Inserted tuple with RID<%d, %d>\n", rid.pageNo.pid, rid.slotNo);

                int inserting = 0;
                // For every attribute of type 100D-vector, update the single LSHF index.
                for (int i = 0; i < numAttrs; i++) {
                    // System.out.println("attrType: " + attrTypes[i].attrType);
                    if (attrTypes[i].attrType == AttrType.attrVector100D) {
                        // Assume a getter exists: get100DVectFld(int fldNo) returning a Vector100Dtype.
                        inserting++;
                        System.out.println("Inserting vector "+ inserting);
                        Vector100Dtype vector = tuple.get100DVectFld(i + 1);
                        vectorIndex.insert(vector, rid);
                        System.out.printf("Updated LSHF index for attribute %d with RID<%d, %d>\n", i, rid.pageNo.pid, rid.slotNo);
                    }
                }

                if (DEBUG) {
                    System.out.println("==========Tuple Inserted=========");
                }
            } // end while

            br.close();
            // vectorIndex.printForest();
            vectorIndex.close();
            SystemDefs.JavabaseBM.flushAllPages();  // write back to disk

            // Test the LSHFIndexFile scan and data check.
            System.out.println("Testing LSHF index for 100D vectors.");
            // Get a composite scan over the entire index (all layers).
            // iterator.Iterator scan = vectorIndex.LSHFFileScan();
            // int count = 0;
            // while (true) {
            //     Tuple t = scan.get_next();
            //     if (t == null)
            //         break;
            //     count++;
            // }
            // System.out.println("Total entries in LSHF index: " + count);

            // // Check if data exists on disk.
            // boolean dataOnDisk = vectorIndex.isDataOnDisk();
            // System.out.println("LSHF index has data on disk? " + dataOnDisk);

            // At the end of batch insertion, output the disk I/O counts.
            System.out.println("Page reads: " + PCounter.rcounter);
            System.out.println("Page writes: " + PCounter.wcounter);

            // test reading LSHF header
            LSHFIndexFile test = new LSHFIndexFile(LSHFIndexFileName);
            test.printAValues();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


}
