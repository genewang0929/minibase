package lshfindex;

import bufmgr.*;
import global.AttrType;
import global.RID;
import global.Vector100Dtype;
import heap.Tuple;
import iterator.*;
import java.io.IOException;
import java.util.Random;

/**
 * LSHFIndexFile implements a fully complete self-tuning LSH-Forest index.
 *
 * For each of the L layers, we maintain a prefix tree (built using the btree package
 * as a template) that stores keys and pointers. Index pages in these trees hold
 * <key, PageID> pairs while leaf pages hold <key, RID> pairs.
 *
 * The hash function for Euclidean distance is defined as:
 *    h(x) = floor((a * x + b) / W)
 * where 'a' is drawn from a Gaussian (N(0,1)) distribution and b is drawn uniformly
 * from [0, W]. The final key is a concatenation of h such values.
 */
public class LSHFIndexFile {

    private static boolean DEBUG = true;

    private String fileName;
    private int h;        // number of hash functions per layer (i.e., number of hash values to concatenate)
    private int L;        // number of layers
    private LSHFPrefixTree[] prefixTrees;  // one prefix tree per layer

    // For each layer and each hash function, we store a random projection vector "a"
    // and a corresponding offset "b". They are used to compute:
    //   h(x) = floor((a * x + b) / W)
    // aValues[layer][i] is a 100-dimensional vector (array of doubles)
    private double[][][] aValues;
    // bOffsets[layer][i] is the offset for the i-th hash function in the given layer.
    private double[][] bOffsets;

    // Bucket width parameter from Andoni Indyk. This controls the quantization.
    private final double W = 5000.0;

    private Random rand;

    protected final static int BUCKET_NUM = 10;

    /**
     * Constructs a new LSHFIndexFile.
     * @param fileName the base file name used to create the prefix tree files.
     * @param h the number of hash functions (values to concatenate) per layer.
     * @param L the number of layers.
     * @throws Exception if initialization fails.
     */
    public LSHFIndexFile(String fileName, int h, int L) throws Exception {
        this.fileName = fileName;
        this.h = h;
        this.L = L;
        this.rand = new Random();

        // Initialize one prefix tree per layer.
        prefixTrees = new LSHFPrefixTree[L];
        for (int l = 0; l < L; l++) {
            // Each prefix tree will use a file name unique to the layer.
            prefixTrees[l] = new LSHFPrefixTree(fileName + "_layer" + l, h);
        }

        // Allocate arrays for aValues and bOffsets.
        aValues = new double[L][h][100];   // For each layer, for each hash function, a 100D vector.
        bOffsets = new double[L][h];         // For each layer, for each hash function, an offset b.

        // Initialize aValues and bOffsets.
        for (int l = 0; l < L; l++) {
            for (int i = 0; i < h; i++) {
                for (int j = 0; j < 100; j++) {
                    // Use a Gaussian distribution for aValues.
                    aValues[l][i][j] = Math.max(-1, Math.min(1, rand.nextGaussian() / 3));
                }
                // b is drawn uniformly from [1, W).
                bOffsets[l][i] = rand.nextDouble() * (W - 1) + 1;
            }
        }
    }

    /**
     * Computes the concatenated hash for a given vector on a specific layer using a specified prefix length.
     * The computed key is a string concatenation of h_i(x) values (each an integer bucket)
     * computed as: floor((a_i * x + b_i) / W)
     *
     * @param vector the 100D vector.
     * @param layer the layer for which the hash is computed.
     * @param prefixLength the number of hash functions to use (allows self-tuning).
     * @return a String representing the concatenated hash value.
     */
    public String computeHash(Vector100Dtype vector, int layer, int prefixLength) {
        short[] dims = vector.getDimension();
        StringBuilder sb = new StringBuilder();
        int len = Math.min(prefixLength, h);

        for (int i = 0; i < len; i++) {
            double dot = 0.0;
            for (int j = 0; j < 100; j++) {
                dot += aValues[layer][i][j] * dims[j];
            }
            // Compute the hash value using floor((dot + b) / W)
            int hashVal = (int)(Math.floor(Math.abs(dot + bOffsets[layer][i]) % BUCKET_NUM));
            sb.append(hashVal);
            if (i < len - 1) {
                sb.append("_"); // separator between hash values
            }
        }

        // double dot = 0.0;
        // for (int j = 0; j < 100; j++) {
        //     dot += aValues[layer][hashno][j] * dims[j];
        // }
        // // Compute the hash value using floor((dot + b) / W)
        // int hashVal = (int)Math.floor((dot + bOffsets[layer][hashno]) / W);
        // sb.append(hashVal);

        return sb.toString();
    }

    /**
     * Inserts a new 100D vector (with its corresponding RID) into the index.
     * For each layer, the full h-value hash is computed from the vector, then a Vector100DKey is created
     * and inserted into that layer's prefix tree.
     *
     * @param vector the 100D vector to insert.
     * @param rid the record identifier for the corresponding data tuple.
     * @throws Exception if insertion fails.
     */
    public void insert(Vector100Dtype vector, RID rid) throws Exception {
        for (int layer = 0; layer < L; layer++) {
            boolean inserted = false;
            // Compute the full h-value hash for this layer.
            String hashVal = computeHash(vector, layer, h);
            // Create a Vector100DKey using the computed hash string.
            Vector100DKey key = new Vector100DKey(hashVal);
            // Insert into the corresponding prefix tree.

            prefixTrees[layer].insert(key, rid);
        }
    }


    // public void printForest() {
    //     for (int i = 0; i < L; i++) {
    //         try {
    //             LSHF.printPrefixTree(prefixTrees[i].getHeaderPage());
    //         } catch (Exception e) {
    //             System.out.println("[LSHFIndexFile] printForest(): IOerror");
    //         }
    //     }
    // }


    /**
     * Checks if data has actually been stored on disk for the entire LSH forest.
     * It does so by opening a FileScan on each layer's heap file and verifying that at least
     * one tuple is present.
     *
     * @return true if any data is found; false if all layers are empty.
     * @throws Exception if the check fails.
     */
    public boolean isDataOnDisk() throws Exception {
        AttrType[] in1 = new AttrType[3];
        in1[0] = new AttrType(AttrType.attrString);
        in1[1] = new AttrType(AttrType.attrInteger);
        in1[2] = new AttrType(AttrType.attrInteger);
        short[] s_sizes = new short[1];
        s_sizes[0] = 50;
        short len_in1 = 3;
        int n_out_flds = 3;
        FldSpec[] proj_list = new FldSpec[3];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        proj_list[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        CondExpr[] outFilter = null;

        for (int l = 0; l < L; l++) {
            String layerFileName = fileName + "_layer" + l;
            FileScan fs = new FileScan(layerFileName, in1, s_sizes, len_in1, n_out_flds, proj_list, outFilter);
            Tuple t = fs.get_next();
            fs.close();
            if (t != null) {
                return true;
            }
        }
        return false;
    }


    /** Close the LSHF index file.  Unpin header page.
     *@exception PageUnpinnedException  error from the lower layer
     *@exception InvalidFrameNumberException  error from the lower layer
     *@exception HashEntryNotFoundException  error from the lower layer
     *@exception ReplacerException  error from the lower layer
     */
    public void close()
    throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, IOException {
        for (int l = 0; l < L; l++) {
            prefixTrees[l].closePrefixTree();
            System.out.println("Prefix Tree " + l + " closed.");
        }
    }
}
