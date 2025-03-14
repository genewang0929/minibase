import diskmgr.PCounter;
import global.AttrType;
import global.RID;
import global.SystemDefs;
import global.Vector100Dtype;
import heap.*;

import java.io.*;
import java.util.*;

import static global.GlobalConst.NUMBUF;


// Main class for batch insertion
public class batchinsert {

    public static void main(String[] args) {
        if(args.length != 4) {
            System.err.println("Usage: batchinsert <h> <L> <DATAFILENAME> <DBNAME>");
            System.exit(1);
        }

        PCounter.initialize();


        try {
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
                    case 3: attrTypes[i] = new AttrType(AttrType.attrString); break; // You might need to handle string size later
                    case 4: attrTypes[i] = new AttrType(AttrType.attrVector100D); break;
                    default: throw new IOException("Unknown attribute type code: " + typeCode);
                }
            }

            save_attrTypes(heapFilename, numAttrs, attrTypes);

            // TODO: For each attribute of type 100D-vector (represented by "4"), create an LSHForest index.
            // The index file name is the DBNAME followed by the attribute number, h, and L.
//            Map<Integer, LSHForest> lshIndexes = new HashMap<>();
//            for (int i = 0; i < numAttrs; i++) {
//                if (attrTypes[i] == 4) {
//                    String indexFileName = dbName + "_" + i + "_" + h + "_" + L;
//                    lshIndexes.put(i, new LSHForest(h, L, indexFileName));
//                }
//            }

            // Initialize DB
            SystemDefs sysdef = new SystemDefs(dbpath, 500, NUMBUF, "Clock" );


            // Create the heap file for storing tuples (database file)
            Heapfile hf = new Heapfile(heapFilename);

            // Process tuples.
            // Each tuple is stored in the file as n consecutive lines.
            String line;
	    boolean eof = false;
            while ((line = br.readLine()) != null) {
                // Read one tuple; first attribute already in 'line'
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

		if (eof == true) {
		    break;
		}

                // Create a tuple with numAttrs fields.
                Tuple tuple = create_tuple(numAttrs, attrTypes, tupleValues);

                // Convert the tuple to a byte array and insert it into the heap file.
                byte[] tupleData = tuple.getTupleByteArray();
                RID rid = hf.insertRecord(tupleData);
                System.out.printf("Inserted tuple with RID<%d, %d>\n", rid.pageNo.pid, rid.slotNo);

                // TODO: For every attribute of type 100D-vector, update the corresponding LSHForest index.
//                for (int i = 0; i < numAttrs; i++) {
//                    if (attrTypes[i] == 4) {
//                        // Retrieve the vector from the tuple.
//                        Vector100Dtype vector = (Vector100Dtype) tuple.getField(i);
//                        LSHForest index = lshIndexes.get(i);
//                        if (index != null) {
//                            index.insert(vector, rid);
//                        }
//                    }
//                }

            } // end while
            br.close();
            SystemDefs.JavabaseBM.flushAllPages();  // write back to disk

            // At the end of batch insertion, output the disk I/O counts.
            System.out.println("Page reads: " + PCounter.rcounter);
            System.out.println("Page writes: " + PCounter.wcounter);
        }
        catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Tuple create_tuple(int numAttrs, AttrType[] attrTypes, String[] tupleValues) throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException {
        Tuple tuple = new Tuple();
        short[] Ssizes = new short[1];
        Ssizes[0] = 30;
        tuple.setHdr((short) numAttrs, attrTypes, Ssizes);
        for (int i = 0; i < numAttrs; i++) {
            switch(attrTypes[i].attrType) {
                case AttrType.attrInteger: // integer
		    System.out.println("attrType: " + attrTypes[i].attrType);
                    int intVal = Integer.parseInt(tupleValues[i].trim());
                    tuple.setIntFld(i + 1, intVal);
                    break;
                case AttrType.attrReal: // real (float)
		    System.out.println("attrType: " + attrTypes[i].attrType);
                    float floatVal = Float.parseFloat(tupleValues[i].trim());
                    tuple.setFloFld(i + 1, floatVal);
                    break;
                case AttrType.attrString: // string
		    System.out.println("attrType: " + attrTypes[i].attrType);
                    String strVal = tupleValues[i].trim();
                    tuple.setStrFld(i + 1, strVal);
                    break;
                case AttrType.attrVector100D: // 100D-vector
		    System.out.println("attrType: " + attrTypes[i].attrType);

                    // The line should contain 100 integers separated by whitespace
                    String[] vecTokens = tupleValues[i].trim().split("\\s+");
                    if (vecTokens.length != 100) {
                        System.err.println("Error: Expected 100 integers for 100D-vector, found " + vecTokens.length);
                        System.exit(1);
                    }
                    int[] dims = new int[100];
                    for (int j = 0; j < 100; j++) {
			float temp = Float.parseFloat(vecTokens[i].trim());
			dims[j] = (int) temp;
                    }
                    Vector100Dtype vector = new Vector100Dtype(dims);
//                    System.out.println("fldNo: " + (i + 1));
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
        String remove_joincmd = remove_cmd + dbpath;
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
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
            System.exit(1); // Or handle error appropriately
        }
    }
}
