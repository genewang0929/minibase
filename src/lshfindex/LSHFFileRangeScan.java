package LSHFIndex;

import heap.Heapfile;
import iterator.Scan;
import iterator.Tuple;
import global.Vector100Dtype;
import global.RID;
import java.util.LinkedList;
import java.util.Queue;

/**
 * LSHFFileRangeScan implements a range scan over an LSH-Forest index.
 * Given a query key and a Hamming distance threshold, it returns all candidate tuples
 * from the underlying index that are within the specified threshold in hash space.
 */
public class LSHFFileRangeScan {
    private LSHFIndexFile indexFile;
    private Vector100DKey queryKey;
    private int threshold;
    
    // Queue to store tuples that meet the distance criterion.
    private Queue<Tuple> candidateQueue;
    
    /**
     * Constructor for the range scan.
     *
     * @param indexFile the LSHFIndexFile instance.
     * @param queryKey the query key (which wraps a Vector100Dtype).
     * @param threshold the maximum Hamming distance allowed.
     * @throws Exception if the scan fails.
     */
    public LSHFFileRangeScan(LSHFIndexFile indexFile, Vector100DKey queryKey, int threshold) throws Exception {
        this.indexFile = indexFile;
        this.queryKey = queryKey;
        this.threshold = threshold;
        candidateQueue = new LinkedList<>();
        buildCandidateQueue();
    }
    
    /**
     * Helper method to compute the Hamming distance between two binary strings.
     * The Hamming distance is the number of positions at which the corresponding bits differ.
     *
     * @param s1 first binary string.
     * @param s2 second binary string.
     * @return the Hamming distance.
     */
    private int hammingDistance(String s1, String s2) {
        int dist = 0;
        int len = Math.min(s1.length(), s2.length());
        for (int i = 0; i < len; i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                dist++;
            }
        }
        return dist;
    }
    
    /**
     * Builds the candidate queue by scanning the entire index heap file.
     * For each tuple, it computes the Hamming distance between the stored hash (from the tuple)
     * and the query’s hash computed for the same layer. If the distance is within the threshold,
     * the tuple is added to the candidate queue.
     *
     * @throws Exception if scanning fails.
     */
    private void buildCandidateQueue() throws Exception {
        // Open a scan on the underlying heap file.
        Scan scan = indexFile.getHeapfile().openScan();
        Tuple tuple;
        
        // Process each tuple in the index.
        while ((tuple = scan.getNext()) != null) {
            // Retrieve fields from the tuple:
            // Field 1: layer (integer)
            // Field 2: hash value (String)
            int layer = tuple.getIntFld(1);
            String hashValue = tuple.getStringFld(2);
            
            // Compute the query hash for the given layer.
            String queryHash = indexFile.computeHash(queryKey.getVector(), layer);
            
            // Compute the Hamming distance.
            int distance = hammingDistance(queryHash, hashValue);
            
            // If within the threshold, add to the queue.
            if (distance <= threshold) {
                candidateQueue.add(tuple);
            }
        }
        scan.closescan();
    }
    
    /**
     * Returns the next tuple in the range scan.
     * If no more candidates remain, this method returns null.
     *
     * @return the next tuple that meets the threshold criterion, or null if none remain.
     */
    public Tuple getNext() {
        return candidateQueue.poll();
    }
}
