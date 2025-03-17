package lshfindex;

import java.io.*;
import global.*;
import diskmgr.*;
import heap.*;
import btree.*;

/**
 * LSHFSortedPage class
 * Holds records in sorted order based on key similarity measures, using
 * key handling from LSHF.java.
 */
public class LSHFSortedPage extends HFPage {

    private static boolean DEBUG = true;

    int keyType; // Key type, similar to BTSortedPage

    /**
     * Pin the page with pageno and get the corresponding LSHFSortedPage
     * @param pageno the page number the LSHFSortedPage corresponds to
     * @param keyType the key type, expected to be AttrType.attrVector
     * @throws ConstructPageException error in constructing page
     */
    public LSHFSortedPage(PageId pageno, int keyType) throws ConstructPageException {
        super();
        if (DEBUG) {
            System.out.println("[LSHFSortedPage] LSHFSortedPage(PageId pageno, int keyType), page " + pageno.pid);
        }
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false); // Read-disk flag
            this.keyType = keyType;
        } catch (Exception e) {
            throw new ConstructPageException(e, "Construct LSHF sorted page failed");
        }
    }

    /**
     * Associate the LSHFSortedPage instance with an existing Page instance
     * @param page the page to associate with
     * @param keyType the key type
     */
    public LSHFSortedPage(Page page, int keyType) {
        super(page);
        if (DEBUG) {
            System.out.println("[LSHFSortedPage] LSHFSortedPage(Page page, int keyType)");
        }
        this.keyType = keyType;
    }

    /**
     * Create a new page and initialize it as an LSHFSortedPage
     * @param keyType the key type
     * @throws ConstructPageException error in constructing page
     */
    public LSHFSortedPage(int keyType) throws ConstructPageException {
        super();
        if (DEBUG) {
            System.out.println("[LSHFSortedPage] LSHFSortedPage(int keyType)");
        }
        try {
            Page apage = new Page();
            PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
            if (pageId == null) throw new ConstructPageException(null, "New page creation failed");
            this.init(pageId, apage);
            this.keyType = keyType;
        } catch (Exception e) {
            throw new ConstructPageException(e, "Construct LSHF sorted page failed");
        }
    }

    public int getKeyType() {
        return keyType;
    }

    /**
     * Inserts a record in sorted order based on similarity measure
     * @param entry the key-data entry to insert
     * @return the RID where the entry was inserted; null if no space left
     * @throws InsertRecException error when inserting
     */
    protected RID insertRecord(LSHFKeyDataEntry entry) throws InsertRecException {
        int i;
        short nType;
        RID rid;
        byte[] record;

        try {
            record = LSHF.getBytesFromEntry(entry);
            rid = super.insertRecord(record);
            if (rid == null) return null;

            if (entry.data instanceof LeafData)
                nType = NodeType.LEAF;
            else
                nType = NodeType.INDEX;

            // Insertion sort for maintaining order
            for (i = getSlotCnt() - 1; i > 0; i--) {
                KeyClass key_i, key_iplus1;
                key_i = LSHF.getEntryFromBytes(getpage(), getSlotOffset(i),
                                               getSlotLength(i), keyType, nType).key;
                key_iplus1 = LSHF.getEntryFromBytes(getpage(), getSlotOffset(i - 1),
                                                    getSlotLength(i - 1), keyType, nType).key;

                if (LSHF.keyCompare(key_i, key_iplus1) < 0) {
                    // Swap slots
                    int ln = getSlotLength(i);
                    int off = getSlotOffset(i);
                    setSlot(i, getSlotLength(i - 1), getSlotOffset(i - 1));
                    setSlot(i - 1, ln, off);
                } else {
                    break;
                }
            }

            rid.slotNo = i;
            return rid;
        } catch (Exception e) {
            throw new InsertRecException(e, "Insert record failed");
        }
    }

    /**
     * Deletes a record from the LSHFSortedPage and compacts the slot directory
     * @param rid the record ID to delete
     * @return true if deletion succeeds, false if rid is invalid
     * @throws DeleteRecException error when deleting
     */
    public boolean deleteSortedRecord(RID rid) throws DeleteRecException {
        try {
            deleteRecord(rid);
            compact_slot_dir();
            return true;
        } catch (Exception e) {
            if (e instanceof InvalidSlotNumberException)
                return false;
            else
                throw new DeleteRecException(e, "Delete record failed");
        }
    }

    /**
     * Returns the number of records in the page
     * @return the number of records
     * @throws IOException I/O error
     */
    public int numberOfRecords() {
        try {
            return getSlotCnt();
        } catch (Exception e) {
            System.out.println("get number of records failed");
        }
        return 0;
    }
}
