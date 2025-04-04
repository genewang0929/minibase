package lshfindex;

import java.io.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import heap.*;
import btree.*;
import java.util.Arrays;

/**
 * LSHFIndexPage is an index page in the LSH-Forest prefix tree.
 * It stores <key, PageID> pairs, where key is the hash value (of type Vector100DKey)
 * computed for 100D vectors and PageID is the pointer to the child node.
 * 
 * We extend BTSortedPage to reuse the sorted storage mechanism.
 * Left link functionality is omitted because the LSH scheme does not require it.
 */
public class LSHFIndexPage extends LSHFSortedPage {

  public static boolean DEBUG = false;

  public final static int BUCKET_NUM = 4;

  /**
   * Constructs an LSHFIndexPage by pinning the page with the given PageId.
   * Sets the node type to INDEX.
   * @param pageno the PageId of the index page.
   * @param keyType the key type (should be AttrType.attrString for our hash keys).
   * @throws IOException if an I/O error occurs.
   * @throws ConstructPageException if page construction fails.
   */
  public LSHFIndexPage(PageId pageno, int keyType) 
      throws IOException, ConstructPageException {
    super(pageno, keyType);
    setType(NodeType.INDEX);
  }

  /**
   * Constructs an LSHFIndexPage by associating it with an existing Page.
   * Sets the node type to INDEX.
   * @param page the Page instance.
   * @param keyType the key type.
   * @throws IOException if an I/O error occurs.
   * @throws ConstructPageException if page construction fails.
   */
  public LSHFIndexPage(Page page, int keyType) 
      throws IOException, ConstructPageException {
    super(page, keyType);
    setType(NodeType.INDEX);
  }

  /**
   * Constructs a new LSHFIndexPage (allocating a new page).
   * Sets the node type to INDEX.
   * @param keyType the key type.
   * @throws IOException if an I/O error occurs.
   * @throws ConstructPageException if page construction fails.
   */
  public LSHFIndexPage(int keyType) 
      throws IOException, ConstructPageException {
    super(keyType);
    setType(NodeType.INDEX);
  }

  // public void setBucketPageId(String key, PageId pageNo, int level) {
  //   int[] hashArray = parseHashValue(key);
  //   this.bucket_list[hashArray[level]] = pageNo;
  // }

  // public PageId getBucketPageId(int bucketNo) {
  //   return this.bucket_list[bucketNo];
  // }

  // public int getBucketNo(PageId pageno) {
  //   for (int i = 0;)
  //   return
  // }

  /**
   * Inserts a <key, PageID> entry into the index page.
   * @param key the key (hash value) to be inserted.
   * @param pageNo the PageId pointer to the child node.
   * @return the RID where the entry was inserted, or null if no space left.
   * @throws IndexInsertRecException if insertion fails.
   */
  public RID insertKey(Vector100DKey key, PageId pageNo, int level) 
      throws IndexInsertRecException {
    RID rid;
    LSHFKeyDataEntry entry;
    try {
      entry = new LSHFKeyDataEntry(key.getKey(), pageNo);
      rid = super.insertRecord(entry);
      // setBucketPageId(key.getKey(), pageNo, level);
      return rid;
    } catch (Exception e) {
      throw new IndexInsertRecException(e, "Insert failed");
    }
  }

  private int[] parseHashValue(String key, int keySize) {
    int[] hashArray = new int[keySize];
    for (int i = 0; i < keySize; i++) {
      hashArray[i] = Integer.parseInt(key.split("_")[i]);
    }
    return hashArray;
  }
  
  /**
   * Searches for the child pointer (PageID) corresponding to the given key.
   * The search is performed by scanning the sorted entries in the index page.
   * @param key the key used in the search.
   * @return the PageId of the child to be searched next.
   * @throws IndexSearchException if the search fails.
   */
  public PageId getPageNoByKey(Vector100DKey key) 
      throws IndexSearchException
  {
    LSHFKeyDataEntry entry;
    int i;
    try {
      for (i = getSlotCnt() - 1; i >= 0; i--) {
        entry = LSHF.getEntryFromBytes(getpage(), getSlotOffset(i), 
                                     getSlotLength(i), this.getKeyType(), NodeType.INDEX);
        // if (DEBUG) {
        //   System.out.println("[LSHFIndexPage] getPageNoByKey(): key = " + key.getKey());
        //   System.out.println("[LSHFIndexPage] getPageNoByKey(): entry.key = " + ((Vector100DKey)(entry.key)).getKey());
        // }
        if (LSHF.keyCompare(key, entry.key) >= 0) {
          return ((IndexData)entry.data).getData();
        }
      }
      // If key is less than all entries, return the default pointer.
      if (DEBUG) {
        System.out.println("[LSHFIndexPage] getPageNoByKey(): returning -1");
      }
      return getPrevPage();
    } catch (Exception e) {
      throw new IndexSearchException(e, "Get entry failed");
    }
  }

  // public int[] parseHashValue(String hashvalue) throws NumberFormatException {
  //   if (hashvalue == null || hashvalue.isEmpty()) {
  //     throw new IllegalArgumentException("Hash value cannot be null or empty");
  //   }
  //   // Split the string by underscore
  //   String[] parts = hashValue.split("_");
  //   // Convert each part to an integer
  //   int[] hashArray = Arrays.stream(parts).mapToInt(Integer::parseInt).toArray();
    
  //   return hashArray;
  // }

  /**
   * Returns the first key-data entry on this index page.
   * @param rid an output parameter that will contain the RID of the first entry.
   * @return the first KeyDataEntry, or null if no entries exist.
   * @throws IteratorException if an iteration error occurs.
   */
  public LSHFKeyDataEntry getFirst(RID rid) 
      throws IteratorException {
    LSHFKeyDataEntry entry; 
    try { 
      rid.pageNo = getCurPage();
      rid.slotNo = 0; // start with first slot
      if (getSlotCnt() == 0) {
        if (DEBUG) {
          System.out.println("[LSHFIndexPage] getFirst(): getSlotCnt() = 0");
        }
        return null;
      }
      entry = LSHF.getEntryFromBytes(getpage(), getSlotOffset(0), 
                                   getSlotLength(0), this.getKeyType(), NodeType.INDEX);
      return entry;
    } catch (Exception e) {
      throw new IteratorException(e, "Get first entry failed");
    }
  }
  
  /**
   * Returns the next key-data entry on this index page.
   * @param rid an input/output parameter containing the current RID; it is advanced.
   * @return the next KeyDataEntry, or null if no more entries.
   * @throws IteratorException if an iteration error occurs.
   */
  public LSHFKeyDataEntry getNext(RID rid)
      throws IteratorException {
    LSHFKeyDataEntry entry; 
    int i;
    try {
      rid.slotNo++; // advance to next slot
      i = rid.slotNo;
      if (rid.slotNo >= getSlotCnt()) {
        return null;
      }
      entry = LSHF.getEntryFromBytes(getpage(), getSlotOffset(i), 
                                   getSlotLength(i), this.getKeyType(), NodeType.INDEX);
      return entry;
    } catch (Exception e) {
      throw new IteratorException(e, "Get next entry failed");
    }
  }
  
  // Left link methods are omitted because the LSH-Forest prefix tree uses hashing
  // to route searches rather than leftmost pointers.
}
