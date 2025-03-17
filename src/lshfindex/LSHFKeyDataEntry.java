package lshfindex;

import global.*;
import btree.*; // For compatibility with DataClass, RID, PageId, etc.

/**
 * LSHFKeyDataEntry: Defines a (key, data) pair for the LSH-Forest.
 * In LSHF, keys are always of type Vector100DKey and data is either:
 *   - an IndexData wrapping a PageId (for index entries) or
 *   - a LeafData wrapping a RID (for leaf entries).
 */
public class LSHFKeyDataEntry {
    /** The key in the (key, data) pair */
    public KeyClass key;
    /** The data in the (key, data) pair */
    public DataClass data;
    
    /** Constructor for an index entry, where the key is a Vector100DKey and data is a PageId */
    public LSHFKeyDataEntry(KeyClass key, PageId pageNo) {
        if (key instanceof Vector100DKey) {
            this.key = new Vector100DKey(((Vector100DKey) key).getKey());
        }
        else {
            throw new IllegalArgumentException("Key must be of type Vector100DKey");
        }
        this.data = new IndexData(pageNo);
    }
    
    /** Constructor for a leaf entry, where the key is a Vector100DKey and data is a RID */
    public LSHFKeyDataEntry(KeyClass key, RID rid) {
        if (key instanceof Vector100DKey) {
            this.key = new Vector100DKey(((Vector100DKey) key).getKey());
        }
        else {
            throw new IllegalArgumentException("Key must be of type Vector100DKey");
        }
        this.data = new LeafData(rid);
    }
    
    /** Constructor for a (key, data) pair, copying both key and data.\n"
     *  This assumes the key is a Vector100DKey and data is either IndexData or LeafData.\n"
     */
    public LSHFKeyDataEntry(KeyClass key, DataClass data) {
        if (key instanceof Vector100DKey)
            this.key = new Vector100DKey(((Vector100DKey) key).getKey());
        else
            throw new IllegalArgumentException("Key must be of type Vector100DKey");
        
        if (data instanceof IndexData)
            this.data = new IndexData(((IndexData) data).getData());
        else if (data instanceof LeafData)
            this.data = new LeafData(((LeafData) data).getData());
        else
            throw new IllegalArgumentException("Data must be either IndexData or LeafData");
    }

    public LSHFKeyDataEntry(String hashvalue, PageId pageNo) {
        this.key = new Vector100DKey(hashvalue);
        this.data = new IndexData(pageNo);
    }

    public LSHFKeyDataEntry(String hashvalue, RID rid) {
        this.key = new Vector100DKey(hashvalue);
        this.data = new LeafData(rid);
    }
    
    /**
     * Shallow equality check between this entry and another.
     * @param entry the other LSHFKeyDataEntry to compare against.
     * @return true if both key and data match, false otherwise.
     */
    public boolean equals(LSHFKeyDataEntry entry) {
        boolean keysEqual = ((Vector100DKey) key).getKey().equals(((Vector100DKey) entry.key).getKey());
        boolean dataEqual;
        if (data instanceof IndexData) {
            dataEqual = (((IndexData) data).getData().pid == ((IndexData) entry.data).getData().pid);
        } else { // assume LeafData
            dataEqual = ((RID)((LeafData) data).getData()).equals(((RID)((LeafData) entry.data).getData()));
        }
        return keysEqual && dataEqual;
    }
}
