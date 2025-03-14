package LSHFIndex;

import iterator.*;
import heap.Tuple;
import java.io.IOException;
import java.util.Iterator;

/**
 * LSHFCompositeScan is a simple wrapper scan that combines tuples from multiple 
 * prefix tree files. It implements the Iterator interface (from the iterator package)
 * and wraps a Java Iterator over Tuple objects.
 */
public class LSHFCompositeScan extends iterator.Iterator {
    private java.util.Iterator<Tuple> iter;
    private Tuple nextTuple;
    
    public LSHFCompositeScan(java.util.Iterator<Tuple> iter) {
        this.iter = iter;
        advance();
    }
    
    private void advance() {
        if (iter.hasNext()) {
            nextTuple = iter.next();
        } else {
            nextTuple = null;
        }
    }
    
    /**
     * Returns the next tuple in the composite scan.
     * @return the next Tuple or null if none.
     * @throws IOException if an I/O error occurs.
     */
    public Tuple get_next() throws IOException {
        Tuple ret = nextTuple;
        advance();
        return ret;
    }
    
    /**
     * Closes the scan.
     */
    public void close() {
        // No resources to free in this simple implementation.
        closeFlag = true;
    }
}
