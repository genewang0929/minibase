package LSHFIndex;

import java.io.*;
import global.*;
import diskmgr.*;
import heap.*;
import btree.*;

/**
 * LSHFLeafPage is a leaf page in the LSH-Forest prefix tree.
 * It stores <key, RID> pairs where the key is the hash value (a binary string)
 * computed from a 100D vector. Overflow pages are handled via BTSortedPage.
 */
public class LSHFLeafPage extends BTSortedPage {

  /**
   * Constructs an LSHFLeafPage by pinning the page with the given PageId.
   * Sets the node type to LEAF.
   * @param pageno the PageId of the leaf page.
   * @param keyType the type of key (should be AttrType.attrString for our hash keys).
   * @throws IOException if an I/O error occurs.
   * @throws ConstructPageException if construction fails.
   */
  public LSHFLeafPage(PageId pageno, int keyType)
      throws IOException, ConstructPageException {
    super(pageno, keyType);
    setType(NodeType.LEAF);
  }

  /**
   * Constructs an LSHFLeafPage by associating it with an existing Page.
   * Sets the node type to LEAF.
   * @param page the Page instance.
   * @param keyType the type of key.
   * @throws IOException if an I/O error occurs.
   * @throws ConstructPageException if construction fails.
   */
  public LSHFLeafPage(Page page, int keyType)
      throws IOException, ConstructPageException {
    super(page, keyType);
    setType(NodeType.LEAF);
  }

  /**
   * Constructs a new LSHFLeafPage (allocating a new page).
   * Sets the node type to LEAF.
   * @param keyType the type of key.
   * @throws IOException if an I/O error occurs.
   * @throws ConstructPageException if construction fails.
   */
  public LSHFLeafPage(int keyType)
      throws IOException, ConstructPageException {
    super(keyType);
    setType(NodeType.LEAF);
  }

  /**
   * Inserts a <key, RID> entry into the leaf page.
   * @param key the key (hash value) to be inserted.
   * @param dataRid the RID of the data record.
   * @return the RID where the record was inserted; null if no space is available.
   * @throws LeafInsertRecException if insertion fails.
   */
  public RID insertRecord(KeyClass key, RID dataRid)
      throws LeafInsertRecException {
    KeyDataEntry entry;
    try {
      entry = new KeyDataEntry(key, dataRid);
      return super.insertRecord(entry);
    } catch (Exception e) {
      throw new LeafInsertRecException(e, "insert record failed");
    }
  }

  /**
   * Returns the first key-data entry in the leaf page.
   * @param rid an output parameter that will hold the RID of the first entry.
   * @return the first KeyDataEntry, or null if the page is empty.
   * @throws IteratorException if an iteration error occurs.
   */
  public KeyDataEntry getFirst(RID rid)
      throws IteratorException {
    KeyDataEntry entry;
    try {
      rid.pageNo = getCurPage();
      rid.slotNo = 0; // begin with first slot
      if (getSlotCnt() <= 0) {
        return null;
      }
      entry = BT.getEntryFromBytes(getpage(),
                                   getSlotOffset(0),
                                   getSlotLength(0),
                                   this.getKeyType(),
                                   NodeType.LEAF);
      return entry;
    } catch (Exception e) {
      throw new IteratorException(e, "Get first entry failed");
    }
  }

  /**
   * Returns the next key-data entry in the leaf page.
   * @param rid an input/output parameter containing the current RID; it is advanced.
   * @return the next KeyDataEntry, or null if no more entries exist.
   * @throws IteratorException if an iteration error occurs.
   */
  public KeyDataEntry getNext(RID rid)
      throws IteratorException {
    KeyDataEntry entry;
    int i;
    try {
      rid.slotNo++; // advance to the next slot
      i = rid.slotNo;
      if (rid.slotNo >= getSlotCnt()) {
        return null;
      }
      entry = BT.getEntryFromBytes(getpage(),
                                   getSlotOffset(i),
                                   getSlotLength(i),
                                   this.getKeyType(),
                                   NodeType.LEAF);
      return entry;
    } catch (Exception e) {
      throw new IteratorException(e, "Get next entry failed");
    }
  }

  /**
   * Returns the current record in the iteration without advancing the iterator.
   * @param rid the current RID; input and output parameter.
   * @return the current KeyDataEntry.
   * @throws IteratorException if an iteration error occurs.
   */
  public KeyDataEntry getCurrent(RID rid)
      throws IteratorException {
    // Decrement slotNo temporarily to re-read the current entry.
    rid.slotNo--;
    return getNext(rid);
  }

  /**
   * Deletes a key-data entry from the leaf page.
   * @param dEntry the entry to be deleted.
   * @return true if the deletion was successful; false if the entry was not found.
   * @throws LeafDeleteException if deletion fails.
   */
  public boolean delEntry(KeyDataEntry dEntry)
      throws LeafDeleteException {
    KeyDataEntry entry;
    RID rid = new RID();
    try {
      for (entry = getFirst(rid); entry != null; entry = getNext(rid)) {
        if (entry.equals(dEntry)) {
          if (super.deleteSortedRecord(rid) == false)
            throw new LeafDeleteException(null, "Delete record failed");
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      throw new LeafDeleteException(e, "delete entry failed");
    }
  }

  // Wrapper for protected method
  // public int getRecordCount() {
  //   return super.numberOfRecords();
  // }
}
