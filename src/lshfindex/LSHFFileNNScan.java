package LSHFIndex;

import heap.Heapfile;
import iterator.Scan;
import iterator.Tuple;
import global.Vector100Dtype;
import global.RID;
import java.util.PriorityQueue;
import java.util.Comparator;

/**
 * LSHFFileNNScan implements a nearest neighbor scan over an LSH-Forest index.
 * Given a query key and a count, it returns the 'count' best candidate tuples
 * from the underlying index based on Hamming distance in the hash space.
 */
public class LSHFFileNNScan {
    private LSHFIndexFile indexFile;
    private Vector100DKey queryKey;
    private int count;
    
    // Priority queue to store candidates ordered by their distance.
    private PriorityQueue<Candidate> candidateQueue;
    // Number of candidates returned so far.
    private int returnedCount = 0;
    
    /**
     * Inner class representing a candidate entry from the index.
     */
    private class Candidate {
        Tuple tuple;
        int distance;
        
        Candidate(Tuple tuple, int distance) {
            this.tuple = tuple;
            this.distance = distance;
        }
    }
    
    /**
     * Comparator that orders candidates by increasing distance.
     */
    private class CandidateComparator implements Comparator<Candidate> {
        public int compare(Candidate a, Candidate b) {
            return Integer.compare(a.distance, b.distance);
        }
    }
    
    /**
     * Constructor for the NN scan.
     *
     * @param indexFile the LSHFIndexFile instance.
     * @param queryKey the query key (which wraps a Vector100Dtype).
     * @param count the number of nearest neighbors to return.
     * @throws Exception if the scan fails.
     */
    public LSHFFileNNScan(LSHFIndexFile indexFile, Vector100DKey queryKey, int count) throws Exception {
        this.indexFile = indexFile;
        this.queryKey = queryKey;
        this.count = count;
        candidateQueue = new PriorityQueue<>(new CandidateComparator());
        buildCandidateQueue();
    }
    
    /**
     * Helper method to compute the Hamming distance between two binary strings.
     * This distance is simply the number of differing bits.
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
     * For each tuple, we compute the query's hash for the corresponding layer and
     * then the Hamming distance between that hash and the stored hash.
     *
     * @throws Exception if scanning fails.
     */
    private void buildCandidateQueue() throws Exception {
        // Open a scan on the underlying heap file.
        Scan scan = indexFile.getHeapfile().openScan();
        Tuple tuple;
        
        // Process each tuple in the index.
        while ((tuple = scan.getNext()) != null) {
            // Retrieve fields from the tuple.
            // Field 1: layer (int)
            // Field 2: hash value (String)
            // Field 3: RID page number (int)
            // Field 4: RID slot number (int)
            int layer = tuple.getIntFld(1);
            String hashValue = tuple.getStringFld(2);
            int pageNo = tuple.getIntFld(3);
            int slotNo = tuple.getIntFld(4);
            RID rid = new RID(pageNo, slotNo);
            
            // For this candidate, compute the query hash for the corresponding layer.
            String queryHash = indexFile.computeHash(queryKey.getVector(), layer);
            // Compute the Hamming distance between the candidate's stored hash and the query's hash.
            int distance = hammingDistance(queryHash, hashValue);
            // Create a candidate object and add it to the queue.
            Candidate candidate = new Candidate(tuple, distance);
            candidateQueue.add(candidate);
        }
        scan.closescan();
    }
    
    /**
     * Returns the next nearest neighbor tuple.
     * If the requested number of neighbors has been returned or no more candidates
     * are available, this method returns null.
     *
     * @return the next tuple, or null if finished.
     * @throws Exception if retrieval fails.
     */
    public Tuple getNext() throws Exception {
        if (returnedCount >= count || candidateQueue.isEmpty()) {
            return null;
        }
        Candidate best = candidateQueue.poll();
        returnedCount++;
        return best.tuple;
    }
}
