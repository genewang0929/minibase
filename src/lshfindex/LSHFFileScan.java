package LSHFIndex;

import heap.Heapfile;
import iterator.Scan;
import iterator.Tuple;

/**
 * LSHFFileScan provides a sequential scan over all tuples in the LSH-Forest index.
 * It simply wraps the underlying heap file scan and returns each tuple one by one.
 */
public class LSHFFileScan {
    private Scan scan;
    
    /**
     * Constructs a new full scan iterator for the given LSHFIndexFile.
     *
     * @param indexFile the LSHFIndexFile instance containing the index.
     * @throws Exception if opening the scan fails.
     */
    public LSHFFileScan(LSHFIndexFile indexFile) throws Exception {
        // Open a scan on the underlying heap file that stores the index entries.
        scan = indexFile.getHeapfile().openScan();
    }
    
    /**
     * Retrieves the next tuple in the scan.
     *
     * @return the next tuple, or null if no more tuples are available.
     * @throws Exception if retrieval fails.
     */
    public Tuple getNext() throws Exception {
        return scan.getNext();
    }
    
    /**
     * Closes the scan and releases any associated resources.
     *
     * @throws Exception if closing the scan fails.
     */
    public void close() throws Exception {
        scan.closescan();
    }
}
