import diskmgr.*;
import global.*;
import heap.*;
import iterator.*;
import java.io.*;
import java.util.*;
import LSHFIndex.*;
import static global.GlobalConst.NUMBUF;

public class insertTest {

    public static void main(String[] args) {
        if(args.length != 4) {
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
            String heapFilename = "batch_file";

            // Clean up before any operation
            String dbpath = "/tmp/"+System.getProperty("user.name")+"."+dbName;
            String logpath = "/tmp/"+System.getProperty("user.name")+".log";
            cleanup(logpath, dbpath);

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
            SystemDefs sysdef = new SystemDefs(dbpath, 500, NUMBUF, "Clock" );

	    // Create the heap file for storing tuples (database file)
            Heapfile hf = new Heapfile(heapFilename);
	    
	    System.out.println("Now try to create LSHFIndexFile:");

            // Create exactly one LSHFIndexFile for all 100D-vector attributes.
            // This index file will store all the 100D vectors from every tuple.
            LSHFIndexFile vectorIndex = new LSHFIndexFile("LSHFIndex_100D", h, L);

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
                    System.out.println("nextLine: " + nextLine);
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

                // For every attribute of type 100D-vector, update the single LSHF index.
                for (int i = 0; i < numAttrs; i++) {
		    System.out.println("attrType: " + attrTypes[i].attrType);
                    if (attrTypes[i].attrType == AttrType.attrVector100D) {
                        // Assume a getter exists: get100DVectFld(int fldNo) returning a Vector100Dtype.
                        Vector100Dtype vector = tuple.get100DVectFld(i + 1);
                        vectorIndex.insert(vector, rid);
                        System.out.printf("Updated LSHF index for attribute %d with RID<%d, %d>\n", i, rid.pageNo.pid, rid.slotNo);
                    }
                }
            } // end while

            br.close();
            vectorIndex.close();
            SystemDefs.JavabaseBM.flushAllPages();  // write back to disk

            // Test the LSHFIndexFile scan and data check.
            System.out.println("Testing LSHF index for 100D vectors.");
            // Get a composite scan over the entire index (all layers).
            iterator.Iterator scan = vectorIndex.LSHFFileScan();
            int count = 0;
            while (true) {
                Tuple t = scan.get_next();
                if (t == null)
                    break;
                count++;
            }
            System.out.println("Total entries in LSHF index: " + count);

            // Check if data exists on disk.
            boolean dataOnDisk = vectorIndex.isDataOnDisk();
            System.out.println("LSHF index has data on disk? " + dataOnDisk);

            // At the end of batch insertion, output the disk I/O counts.
            System.out.println("Page reads: " + PCounter.rcounter);
            System.out.println("Page writes: " + PCounter.wcounter);
        }
        catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Tuple create_tuple(int numAttrs, AttrType[] attrTypes, String[] tupleValues)
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
                    int[] dims = new int[100];
                    for (int j = 0; j < 100; j++) {
                        dims[j] = Integer.parseInt(vecTokens[j].trim());
                    }
                    Vector100Dtype vector = new Vector100Dtype(dims);
                    tuple.set100DVectFld(i + 1, vector);
                    break;
                default:
                    System.err.println("Unknown attribute type: " + attrTypes[i]);
            }
        }
        return tuple;
    }

    private static void cleanup(String logpath, String dbpath) {
        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println(""+e);
        }
    }

    private static void save_attrTypes(String heapFilename, int numAttrs, AttrType[] attrTypes) {
        try (PrintWriter schemaWriter = new PrintWriter(new FileWriter(heapFilename + ".schema"))) {
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
