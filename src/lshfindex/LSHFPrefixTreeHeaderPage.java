package lshfindex;

import java.io.*;
import btree.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

/**
 * LSHFPrefixTreeHeaderPage is the header page for the LSH-Forest prefix tree.
 * This header page stores essential metadata for the prefix tree structure.
 *
 * The header page logically contains the following seven elements:
 *   - magic0: a magic number identifying the file.
 *   - rootId: the page identifier of the tree root.
 *   - keyType: the type of keys stored in the tree.
 *   - maxKeySize: the maximum key size (in bytes).
 *   - deleteFashion: the delete policy for the tree.
 *   - (and any additional metadata as needed)
 *
 * In our implementation, the node type is implicitly defined as a prefix tree header.
 */
public class LSHFPrefixTreeHeaderPage extends HFPage {

  /**
   * Sets the page id for this header page.
   * @param pageno the page identifier.
   */
  public void setPageId(PageId pageno) throws IOException {
    setCurPage(pageno);
  }

  /**
   * Retrieves the page id of this header page.
   * @return the current page identifier.
   */
  public PageId getPageId() throws IOException {
    return getCurPage();
  }

  /**
   * Sets the magic number (magic0) for this header page.
   * @param magic the magic number to set.
   */
  public void set_magic0(int magic) throws IOException {
    setPrevPage(new PageId(magic));
  }

  /**
   * Retrieves the magic number (magic0) from this header page.
   * @return the magic number.
   */
  public int get_magic0() throws IOException {
    return getPrevPage().pid;
  }

  /**
   * Sets the root page id for the prefix tree.
   * @param rootID the root page id.
   */
  public void set_rootId(PageId rootID) throws IOException {
    setNextPage(rootID);
  }

  /**
   * Retrieves the root page id for the prefix tree.
   * @return the root page id.
   */
  public PageId get_rootId() throws IOException {
    return getNextPage();
  }

  /**
   * Sets the key type.
   * @param key_type the key type (for example, a constant representing Vector100DKey).
   */
  public void set_keyType(short key_type) throws IOException {
    setSlot(3, (int) key_type, 0);
  }

  /**
   * Retrieves the key type.
   * @return the key type.
   */
  public short get_keyType() throws IOException {
    return (short) getSlotLength(3);
  }

  /**
   * Sets the maximum key size.
   * @param key_size the maximum key size in bytes.
   */
  public void set_maxKeySize(int key_size) throws IOException {
    setSlot(1, key_size, 0);
  }

  /**
   * Retrieves the maximum key size.
   * @return the maximum key size in bytes.
   */
  public int get_maxKeySize() throws IOException {
    return getSlotLength(1);
  }

  /**
   * Sets the delete fashion (policy) for the tree.
   * @param fashion an integer representing the deletion policy.
   */
  public void set_deleteFashion(int fashion) throws IOException {
    setSlot(2, fashion, 0);
  }

  /**
   * Retrieves the delete fashion (policy).
   * @return the delete fashion.
   */
  public int get_deleteFashion() throws IOException {
    return getSlotLength(2);
  }

  /**
   * Constructor that pins an existing header page using its PageId.
   * @param pageno the PageId of the header page.
   * @throws ConstructPageException if pinning fails.
   */
  public LSHFPrefixTreeHeaderPage(PageId pageno) throws ConstructPageException {
    super();
    try {
      SystemDefs.JavabaseBM.pinPage(pageno, this, false);
    } catch (Exception e) {
      throw new ConstructPageException(e, "pin page failed");
    }
  }

  /**
   * Constructor that wraps an existing Page.
   * @param page the Page object representing the header page.
   */
  public LSHFPrefixTreeHeaderPage(Page page) {
    super(page);
  }

  /**
   * Constructor that creates a new header page.
   * @throws ConstructPageException if a new page cannot be allocated.
   */
  public LSHFPrefixTreeHeaderPage() throws ConstructPageException {
    super();
    try {
      Page apage = new Page();
      PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
      if (pageId == null)
        throw new ConstructPageException(null, "new page failed");
      this.init(pageId, apage);
    } catch (Exception e) {
      throw new ConstructPageException(e, "construct header page failed");
    }
  }
}
