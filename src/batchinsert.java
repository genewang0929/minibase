import java.io.*;
import java.util.*;

// Main class for batch insertion
public class batchinsert {

    public static void main(String[] args) {
        if(args.length != 4) {
            System.err.println("Usage: batchinsert <h> <L> <DATAFILENAME> <DBNAME>");
            System.exit(1);
        }

        try {
            int h = Integer.parseInt(args[0]);
            int L = Integer.parseInt(args[1]);
            String dataFilename = args[2];
            String dbName = args[3];

            // Open the data file for reading
            BufferedReader br = new BufferedReader(new FileReader(dataFilename));

            // The first line is the number of attributes
            int numAttrs = Integer.parseInt(br.readLine().trim());

            // The next line contains the attribute types
            String[] typeTokens = br.readLine().trim().split("\\s+");
            int[] attrTypes = new int[numAttrs];
            for (int i = 0; i < numAttrs; i++) {
                attrTypes[i] = Integer.parseInt(typeTokens[i]);
            }

            // For each attribute of type 100D-vector (represented by "4"),
            // create an LSHForest index.
            // The index file name is the DBNAME followed by the attribute number, h, and L.
            Map<Integer, LSHForest> lshIndexes = new HashMap<>();
            for (int i = 0; i < numAttrs; i++) {
                if (attrTypes[i] == 4) {
                    String indexFileName = dbName + "_" + i + "_" + h + "_" + L;
                    lshIndexes.put(i, new LSHForest(h, L, indexFileName));
                }
            }

            // Create the heap file for storing tuples (database file)
            Heapfile hf = new Heapfile(dbName);

            // Process tuples.
            // Each tuple is stored in the file as n consecutive lines.
            String line;
            while ((line = br.readLine()) != null) {
                // Read one tuple; first attribute already in 'line'
                String[] tupleValues = new String[numAttrs];
                tupleValues[0] = line;
                for (int i = 1; i < numAttrs; i++) {
                    tupleValues[i] = br.readLine();
                }

                // Create a tuple with numAttrs fields.
                Tuple tuple = new Tuple(numAttrs);
                for (int i = 0; i < numAttrs; i++) {
                    switch(attrTypes[i]) {
                        case 1: // integer
                            int intVal = Integer.parseInt(tupleValues[i].trim());
                            tuple.setField(i, intVal);
                            break;
                        case 2: // real (float)
                            float floatVal = Float.parseFloat(tupleValues[i].trim());
                            tuple.setField(i, floatVal);
                            break;
                        case 3: // string
                            String strVal = tupleValues[i].trim();
                            tuple.setField(i, strVal);
                            break;
                        case 4: // 100D-vector
                            // The line should contain 100 integers separated by whitespace
                            String[] vecTokens = tupleValues[i].trim().split("\\s+");
                            if (vecTokens.length != 100) {
                                System.err.println("Error: Expected 100 integers for 100D-vector, found " + vecTokens.length);
                                System.exit(1);
                            }
                            int[] dims = new int[100];
                            for (int j = 0; j < 100; j++) {
                                dims[j] = Integer.parseInt(vecTokens[j].trim());
                            }
                            Vector100Dtype vector = new Vector100Dtype(dims);
                            tuple.setField(i, vector);
                            break;
                        default:
                            System.err.println("Unknown attribute type: " + attrTypes[i]);
                    }
                }

                // Convert the tuple to a byte array and insert it into the heap file.
                byte[] tupleData = tuple.toByteArray();
                RID rid = hf.insertRecord(tupleData);

                // For every attribute of type 100D-vector, update the corresponding LSHForest index.
                for (int i = 0; i < numAttrs; i++) {
                    if (attrTypes[i] == 4) {
                        // Retrieve the vector from the tuple.
                        Vector100Dtype vector = (Vector100Dtype) tuple.getField(i);
                        LSHForest index = lshIndexes.get(i);
                        if (index != null) {
                            index.insert(vector, rid);
                        }
                    }
                }
            } // end while
            br.close();

            // At the end of batch insertion, output the disk I/O counts.
            System.out.println("Page reads: " + PCounter.rcounter);
            System.out.println("Page writes: " + PCounter.wcounter);
        }
        catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
