package LSHFIndex;

import global.Vector100Dtype;
import global.RID;
import btree.KeyClass;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import iterator.*;

/**
 * LSHFIndexFile implements a fully complete self-tuning LSH-Forest index.
 * 
 * For each of the L layers, we maintain a prefix tree (built using the btree package
 * as a template) that stores keys and pointers. Index pages in these trees hold
 * <key, PageID> pairs while leaf pages hold <key, RID> pairs.
 *
 * This implementation modifies the hash function generation as described in:
 * Alexandr Andoni and Piotr Indyk. 2008. Near-optimal hashing algorithms for approximate
 * nearest neighbor in high dimensions.
 *
 * The hash function for Euclidean distance is defined as:
 *    h(x) = floor((a · x + b) / W)
 * where a is drawn from a Gaussian (N(0,1)) distribution and b is drawn uniformly
 * from [0, W]. The final key is a concatenation of h such values.
 */
public class LSHFIndexFile {
    private int h;        // number of hash functions per layer (i.e., number of hash values to concatenate)
    private int L;        // number of layers
    private LSHFPrefixTree[] prefixTrees;  // one prefix tree per layer

    // For each layer and each hash function, we store a random projection vector "a"
    // and a corresponding offset "b". They are used to compute:
    //   h(x) = floor((a · x + b) / W)
    // aValues[layer][i] is a 100-dimensional vector (array of doubles)
    private double[][][] aValues;
    // bOffsets[layer][i] is the offset for the i-th hash function in the given layer.
    private double[][] bOffsets;
    
    // Bucket width parameter from Andoni–Indyk. This controls the quantization.
    private final double W = 5000.0;
    
    private Random rand;

    /**
     * Constructs a new LSHFIndexFile.
     * @param fileName the base file name used to create the prefix tree files.
     * @param h the number of hash functions (values to concatenate) per layer.
     * @param L the number of layers.
     * @throws Exception if initialization fails.
     */
    public LSHFIndexFile(String fileName, int h, int L) throws Exception {
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
                    aValues[l][i][j] = rand.nextGaussian();
                }
                // b is drawn uniformly from [0, W].
                bOffsets[l][i] = rand.nextDouble() * W;
            }
        }
    }

    /**
     * Computes the concatenated hash for a given vector on a specific layer using a specified prefix length.
     * The computed key is a string concatenation of h_i(x) values (each an integer bucket)
     * computed as: floor((a_i · x + b_i) / W)
     *
     * @param vector the 100D vector.
     * @param layer the layer for which the hash is computed.
     * @param prefixLength the number of hash functions to use (allows self-tuning).
     * @return a String representing the concatenated hash value.
     */
    public String computeHash(Vector100Dtype vector, int layer, int prefixLength) {
        int[] dims = vector.getDimension();
        StringBuilder sb = new StringBuilder();
        int len = Math.min(prefixLength, h);
        for (int i = 0; i < len; i++) {
            double dot = 0.0;
            for (int j = 0; j < 100; j++) {
                dot += aValues[layer][i][j] * dims[j];
            }
            // Compute the hash value using floor((dot + b) / W)
            int hashVal = (int)Math.floor((dot + bOffsets[layer][i]) / W);
            sb.append(hashVal);
            if (i < len - 1) {
                sb.append("_"); // separator between hash values
            }
        }
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
            // Compute the full h-value hash for this layer.
            String hashVal = computeHash(vector, layer, h);
            // Create a Vector100DKey using the computed hash string.
            Vector100DKey key = new Vector100DKey(hashVal);
            // Insert into the corresponding prefix tree.
            prefixTrees[layer].insert(key, rid);
        }
    }

    /**
     * Returns a Scan over all entries in the index (from all layers).
     * This method scans each prefix tree and combines the results.
     *
     * @return a Scan object iterating over all index entries.
     * @throws Exception if the scan fails.
     */
    // public Scan LSHFFileScan() throws Exception {
    //     List<RID> allRIDs = new ArrayList<>();
    //     for (int layer = 0; layer < L; layer++) {
    //         List<RID> layerScan = prefixTrees[layer].scanAll();
    //         allRIDs.addAll(layerScan);
    //     }
    //     return new LSHFIndexScan(allRIDs.iterator());
    // }

    /**
     * Performs a range scan using Euclidean distance.
     * For the given query key, each layer's prefix tree is queried for all entries
     * whose keys (with dynamically adjusted prefixes) yield vectors within the specified range.
     *
     * @param queryKey the query key (of type Vector100DKey).
     * @param range the Euclidean distance threshold.
     * @return a Scan over matching entries.
     * @throws Exception if the scan fails.
     */
    // public Scan LSHFFileRangeScan(Vector100DKey queryKey, double range) throws Exception {
    //     List<RID> result = new ArrayList<>();
    //     for (int layer = 0; layer < L; layer++) {
    //         List<RID> layerResult = prefixTrees[layer].rangeSearch(queryKey, range);
    //         result.addAll(layerResult);
    //     }
    //     return new LSHFIndexScan(result.iterator());
    // }

    /**
     * Performs a nearest neighbor (NN) scan.
     * For the given query key, each layer's prefix tree returns the top 'count' candidate RIDs.
     *
     * @param queryKey the query key (of type Vector100DKey).
     * @param count the number of nearest neighbors to return.
     * @return a Scan over the NN candidate entries.
     * @throws Exception if the scan fails.
     */
    // public Scan LSHFFileNNScan(Vector100DKey queryKey, int count) throws Exception {
    //     List<RID> result = new ArrayList<>();
    //     for (int layer = 0; layer < L; layer++) {
    //         List<RID> layerResult = prefixTrees[layer].nnSearch(queryKey, count);
    //         result.addAll(layerResult);
    //     }
    //     return new LSHFIndexScan(result.iterator());
    // }
}
