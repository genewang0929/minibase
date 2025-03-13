package LSHFIndex;

import global.Vector100Dtype;
import heap.Heapfile;
import global.RID;
//import java.util.Random;
import java.util.*;
import iterator.Tuple;
import global.AttrType;

public class LSHFIndexFile {
    private int h; // number of hash functions per layer
    private int L; // number of layers
    private Heapfile indexHeapfile;  // underlying storage for index pages
    private Random random;

    // For each layer, we have h hash functions.
    // Each hash function is represented as an array of random integers (the hyperplane)
    // The structure is: hashFunctions[layer][functionIndex][dimension]
    private int[][][] hashFunctions;
    
    /**
     * Constructs a new LSHFIndexFile.
     * @param fileName the underlying file name for the heap file storage.
     * @param h the number of hash functions per layer.
     * @param L the number of layers.
     * @throws Exception if file creation/opening fails.
     */
    public LSHFIndexFile(String fileName, int h, int L) throws Exception {
        this.h = h;
        this.L = L;
        // Create or open the underlying heap file that stores index pages
        indexHeapfile = new Heapfile(fileName);

        this.random = new Random();
        
        // Initialize hash functions for each layer.
        // For each layer, generate h random hyperplanes (each hyperplane is an array of 100 integers).
        hashFunctions = new int[L][h][100];
        Random rand = new Random();
        for (int l = 0; l < L; l++) {
            for (int i = 0; i < h; i++) {
                for (int j = 0; j < 100; j++) {
                    // Here we generate random integers in a suitable range.
                    // Adjust the range as needed for your application.
                    //hashFunctions[l][i][j] = rand.nextInt(2001) - 1000;  // values between -1000 and 1000
                    hashFunctions[l][i][j] = (int) random.nextGaussian().
                }
            }
        }
    }

    /**
     * Computes the hash value for a given vector on a specific layer.
     * The hash value is computed as an h-bit binary string.
     * For each hash function, if the dot product with the vector is non-negative, the corresponding bit is 1; otherwise, 0.
     *
     * @param vector the 100D vector (of type Vector100Dtype)
     * @param layer the layer for which the hash is computed.
     * @return a String representing the h-bit hash code.
     */
    public String computeHash(Vector100Dtype vector, int layer) {
        int[] dimensions = vector.getDimension();
        StringBuilder sb = new StringBuilder();
        // For each hash function in the layer, compute the dot product with the vector.
        for (int i = 0; i < h; i++) {
            int[] hyperplane = hashFunctions[layer][i];
            long dotProduct = 0;
            for (int j = 0; j < 100; j++) {
                dotProduct += (long) dimensions[j] * hyperplane[j];
            }
            // Append "1" if non-negative, else "0"
            sb.append(dotProduct >= 0 ? "1" : "0");
        }
        return sb.toString();
    }
    
    /**
     * Inserts a new entry into the LSH-Forest index.
     * For each layer, the vector is hashed and the resulting <hash, RID> pair is stored.
     *
     * @param key the key containing the 100D vector (Vector100DKey).
     * @param rid the record id corresponding to the actual data in the data heap file.
     * @throws Exception if the insertion fails.
     */
    public void insert(Vector100DKey key, RID rid) throws Exception {
        // Extract the 100D vector from the key.
        Vector100Dtype vector = key.getKey();
        // For each layer, compute the hash code and insert the entry.
        for (int layer = 0; layer < L; layer++) {
            String hashValue = computeHash(vector, layer);
            // Insert the <layer, hashValue, RID> triple into the heap file.
            // The actual storage format can be a tuple containing these fields.
            insertIntoHeapfile(layer, hashValue, rid);
        }
    }
    
    /**
     * A helper method to insert a tuple into the heap file.
     * This method creates a tuple that contains the layer number, the hash code, and the RID.
     * 
     * The tuple format is as follows:
     *  Field 1: layer (integer)
     *  Field 2: hash value (string) -- length is set to the length of the hash code.
     *  Field 3: RID page number (integer)
     *  Field 4: RID slot number (integer)
     *
     * @param layer the layer number.
     * @param hashValue the computed hash code as a String.
     * @param rid the record identifier pointing to the actual data record.
     * @throws Exception if the insertion into the heap file fails.
     */
    private void insertIntoHeapfile(int layer, String hashValue, RID rid) throws Exception {
        // Create a new tuple with 4 fields.
        Tuple tuple = new Tuple();
        
        // Define the attribute types for the tuple.
        // Field 1: layer (integer)
        // Field 2: hashValue (string)
        // Field 3: RID page number (integer)
        // Field 4: RID slot number (integer)
        AttrType[] attrTypes = new AttrType[4];
        attrTypes[0] = new AttrType(AttrType.attrInteger);
        attrTypes[1] = new AttrType(AttrType.attrString);
        attrTypes[2] = new AttrType(AttrType.attrInteger);
        attrTypes[3] = new AttrType(AttrType.attrInteger);
        
        // Define the string sizes array.
        // For field 2 (hashValue), use its length.
        short[] strSizes = new short[1];
        strSizes[0] = (short) hashValue.length();
        
        // Initialize the tuple header with 4 fields.
        tuple.setHdr((short)4, attrTypes, strSizes);
        
        // Set the fields.
        tuple.setIntFld(1, layer);
        tuple.setStringFld(2, hashValue);
        tuple.setIntFld(3, rid.pageNo);
        tuple.setIntFld(4, rid.slotNo);
        
        // Insert the tuple into the underlying heap file.
        indexHeapfile.insertRecord(tuple.getTupleByteArray());
    }

    public Scan LSHFFileScan() throws Exception {
        return indexHeapfile.getHeapfile().openScan();
    }
}
