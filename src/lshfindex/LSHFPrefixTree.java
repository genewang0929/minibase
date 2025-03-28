package lshfindex;

import btree.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import java.io.*;
import java.util.HashMap;

/**
 * LSHFPrefixTree implements a prefix tree for the LSH-Forest.
 * It uses the btree package template: internal (index) pages hold <key, PageID> pairs
 * and leaf pages hold <key, RID> pairs.
 *
 * The tree supports insertion, scanning all entries, range search, and nearest neighbor search.
 */
public class LSHFPrefixTree {

  private static boolean DEBUG = true;

  private HashMap<PageId, Short> pageTypeMap;

  // File name (base) for this prefix tree (used for header and pages)
  private String fileName;

  // Maximum key size (in our case, the number of hash values concatenated; equals h)
  public int keySize;

  // The key type: our hash keys are strings.
  private int keyType = AttrType.attrString;

  // The header page for the tree
  private LSHFPrefixTreeHeaderPage headerPage;

  private PageId headerPageId;

  // The PageId of the root node.
  // For simplicity, if the tree is new, the root is a leaf page.
  private PageId rootId;

  // A constant for maximum records per leaf page. (In a full implementation, this
  // is determined by the page size.)

  private final static int MAGIC0 = 1989;

  protected final static int MAX_RECORDS_PER_LEAF = 48;

  protected final static int BUCKET_NUM = 10;

  /**
   * Constructs a new LSHFPrefixTree.
   * This constructor creates a new tree with an empty header and no root.
   * @param fileName the base file name to be used for storing tree pages.
   * @param keySize the maximum key length (number of hash values to be concatenated).
   * @throws Exception if tree creation fails.
   */
  public LSHFPrefixTree(String fileName, int keySize) throws Exception {
    if (DEBUG) {
      System.out.println("[LSHFPrefixTree] Creating LSHFPrefixTree " + fileName);
    }
    this.fileName = fileName;
    this.keySize = keySize;

    this.pageTypeMap = new HashMap<>();

    // Create a new header page.
    // In a full system, we would check if the file exists and open it; here we always create a new tree.
    if (DEBUG) {
      System.out.println("[LSHFPrefixTree] Creating header page");
    }

    // get_file_entry from DB.java
    headerPageId = get_file_entry(fileName);
    if (headerPageId == null) {
      headerPage = new LSHFPrefixTreeHeaderPage();
      headerPageId = headerPage.getPageId();
      add_file_entry(fileName, headerPageId);
      headerPage.set_magic0(MAGIC0);
      headerPage.set_rootId(new PageId(-1));
    }


    // header = new LSHFPrefixTreeHeaderPage();
    // // Set an arbitrary magic number.
    // header.set_magic0(0x1234);
    // // Initially, there is no root.
    // rootId = new PageId(-1);
    // header.set_rootId(rootId);

    // // For simplicity, immediately create a new leaf page to serve as the root.
    // LSHFLeafPage leaf = new LSHFLeafPage(keyType);
    // rootId = leaf.getCurPage();
    // header.set_rootId(rootId);
    // if (DEBUG) {
    //   System.out.println("[LSHFPrefixTree] Created root page: " + rootId.pid);
    // }
    // In a full implementation, the header would be written to disk.
  }

  public LSHFPrefixTreeHeaderPage getHeaderPage() {
    return headerPage;
  }

  private void add_file_entry(String fileName, PageId pageno) throws AddFileEntryException {
    try {
      SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
    } catch (Exception e) {
      e.printStackTrace();
      throw new AddFileEntryException(e, "");
    }
  }

  /**
   * Inserts a <key, RID> pair into the prefix tree.
   * For this simple implementation, we assume the tree contains only a single leaf page.
   * In a complete implementation, this method would traverse internal nodes and handle splits.
   * @param key the Vector100DKey (hash value) to insert.
   * @param rid the RID corresponding to the data record.
   * @throws Exception if insertion fails.
   */
  public void insert(Vector100DKey key, RID rid) throws Exception {

    if (DEBUG) {
      System.out.println("===============[LSHFPrefixTree]===============");
      System.out.println("[LSHFPrefixTree] try insert vector with key " + key.getKey());
    }

    LSHFKeyDataEntry newRootEntry;

    if (headerPage.get_rootId().pid == -1) {
      PageId newRootPageId;
      LSHFLeafPage newRootPage;
      RID dummyrid;

      newRootPage = new LSHFLeafPage( headerPage.get_keyType());
      newRootPageId = newRootPage.getCurPage();

      // mapping
      if (DEBUG) {
        if (newRootPageId.pid != -1) {
          setNodeType(newRootPageId, NodeType.LEAF);
          System.out.println("New Root Page " + newRootPageId.pid + " is a leaf page.");
        } else {
          System.out.println("newRootPageId is -1, unexpected.");
        }
      }

      // newRootPage.setNextPage(new PageId(INVALID_PAGE));
      // newRootPage.setPrevPage(new PageId(INVALID_PAGE));


      // ASSERTIONS:
      // - newRootPage, newRootPageId valid and pinned

      newRootPage.insertRecord(key, rid);

      unpinPage(newRootPageId, true); /* = DIRTY */
      updateHeader(newRootPageId);

      return;
    }

    // print
    PageId oldRootPageId = headerPage.get_rootId();

    // unpinPage(oldRootPageId, true);

    newRootEntry = _insert(key, rid, headerPage.get_rootId(), 0);



    if (newRootEntry != null) {
      if (DEBUG) {
        System.out.println("***** [LSHFPrefixTree] newRootEntry check *****");
        System.out.println("newRootEntry.key: " + ((Vector100DKey)(newRootEntry.key)).getKey());
        System.out.println("newRootEntry.data: " + ((IndexData)newRootEntry.data).getData());
      }



      // LSHFIndexPage newRootPage = new LSHFIndexPage(headerPage.get_keyType());
      // PageId newRootPageId = newRootPage.getCurPage();

      // newRootPage.insertKey((Vector100DKey)newRootEntry.key, ((IndexData)newRootEntry.data).getData(), 0);
      PageId newRootPageId = ((IndexData)newRootEntry.data).getData();
      LSHFIndexPage newRootPage = new LSHFIndexPage(newRootPageId, headerPage.get_keyType());

      // update mapping
      if (DEBUG) {
        if (newRootPageId.pid != -1) {
          setNodeType(newRootPageId, NodeType.INDEX);
          System.out.println("Page " + newRootPageId.pid + " is an index page.");
        } else {
          System.out.println("newRootPageId is -1, unexpected.");
        }
      }

      // update mapping
      if (DEBUG) {
        if (oldRootPageId.pid != -1) {
          removeNodeType(oldRootPageId);
          System.out.println("Removing old root page: page " + oldRootPageId.pid);
        } else {
          System.out.println("oldRootPageId is -1, unexpected. Freeing an invalid page.");
        }
      }

      newRootPage.setPrevPage(headerPage.get_rootId());
      unpinPage(newRootPageId, true);
      updateHeader(newRootPageId);
      // unpinPage(oldRootPageId);
      freePage(oldRootPageId);

      try {
        insert(key, rid);
      } catch (Exception e) {
        System.out.println("insert error 1");
      }
    }



    // if (newRootEntry != null) {
    //   LSHFIndexPage newRootPage;
    //   PageId newRootPageId;
    //   Object newEntryKey;

    //   newRootPage = new LSHFIndexPage(keyType);
    //   newRootPageId = newRootPage.getCurPage();

    //   newRootPage.insertKey(newRootEntry.key, ((IndexData)newRootEntry.data).getData(), 0);
    // }

    // setPrevPage?

    if (DEBUG) {
      System.out.println("[LSHFPrefixTree] inserted vector with key " + key.getKey());
    }

    return;
  }


  private void freePage(PageId pageno) throws FreePageException {
    try {
      SystemDefs.JavabaseBM.freePage(pageno);
    } catch (Exception e) {
      throw new FreePageException(e, "");
    }
  }


  private LSHFKeyDataEntry _insert(Vector100DKey key, RID rid, PageId currentPageId, int level)
  throws PinPageException,
           IOException,
           ConstructPageException,
           LeafDeleteException,
           ConstructPageException,
           DeleteRecException,
           IndexSearchException,
           UnpinPageException,
           LeafInsertRecException,
           ConvertException,
           IteratorException,
           IndexInsertRecException,
           KeyNotMatchException,
           NodeNotMatchException,
           InsertException,
    Exception {
    int [] hashArray = parseHashValue(key.getKey());

    LSHFSortedPage currentPage;
    Page page;
    LSHFKeyDataEntry upEntry;

    if (DEBUG) {
      System.out.println("[LSHFPrefixTree] _insert(): entering currentPageId = " + currentPageId);
    }

    page = pinPage(currentPageId);  // unpin
    currentPage = new LSHFSortedPage(page, headerPage.get_keyType());

    // reach leaf page
    if (currentPage.getType() == NodeType.LEAF) {
      LSHFLeafPage currentLeafPage = new LSHFLeafPage(page, headerPage.get_keyType());
      PageId currentLeafPageId = currentPageId;

      // mapping
      if (DEBUG) {
        if (currentLeafPageId.pid != -1) {
          setNodeType(currentLeafPageId, NodeType.LEAF);
          System.out.println("Current Leaf Page " + currentLeafPageId.pid + " is a leaf page.");
        } else {
          System.out.println("currentLeafPageId is -1, unexpected.");
        }
      }

      // base case: no split needed
      if (currentLeafPage.numberOfRecords() < MAX_RECORDS_PER_LEAF) {
        if (DEBUG) {
          System.out.println("[LSHFPrefixTree] _insert(): numberOfRecords = " + currentLeafPage.numberOfRecords());
        }
        currentLeafPage.insertRecord(key, rid); // insert <key, rid>
        unpinPage(currentLeafPageId, true);
        return null;  // (no split)
      }

      // overflow case: all hash functions have been used
      if (level >= keySize) {
        // create overflow page and insert
        if (DEBUG) {
          System.out.println("[LSHFPrefixTree] _insert(): Creating overflow page for leaf page " + currentLeafPageId);
        }
        LSHFLeafPage overflowPage = new LSHFLeafPage(headerPage.get_keyType());
        PageId overflowPageId = overflowPage.getCurPage();
        currentLeafPage.setNextPage(overflowPageId);
        overflowPage.insertRecord(key, rid);  // insert <key, rid>
        unpinPage(currentLeafPageId, true); // not sure if this is needed
        unpinPage(overflowPageId, true);
        return null;
      }
      // else {

      // }

      // split case: reach max record number in a page
      if (DEBUG) {
        System.out.println("[LSHFPrefixTree] _insert(): Splitting leaf page " + currentLeafPageId);
        // try {
        //   LSHF.printPrefixTree(headerPage);
        // } catch (Exception e) {
        //   System.out.println("[LSHFPrefixTree] _insert(): Print prefix tree failed");
        // }
      }

      LSHFIndexPage newIndexPage = new LSHFIndexPage(headerPage.get_keyType()); //need unpin
      PageId newIndexPageId = newIndexPage.getCurPage();

      // update mapping
      if (DEBUG) {
        if (newIndexPageId.pid != -1) {
          setNodeType(newIndexPageId, NodeType.INDEX);
          System.out.println("Page " + newIndexPageId.pid + " is an index page.");
        } else {
          System.out.println("newIndexPageId is -1, unexpected.");
        }
      }

      if (DEBUG) {
        System.out.println("[LSHFPrefixTree] _insert(): newIndexPageId " + newIndexPageId);
      }

      // upEntry = new LSHFKeyDataEntry(Integer.toString(hashArray[level]), newIndexPageId); // ?

      // LSHFLeafPage[] newBuckets = new LSHFLeafPage[BUCKET_NUM];
      // PageId[] newBucketPageIds = new PageId[BUCKET_NUM];

      boolean inserted = false;

      // NEED FIX
      // create buckets for all hash values
      for (int i = 0; i < BUCKET_NUM; i++) {
        LSHFLeafPage newLeafPage;
        PageId newLeafPageId;

        newLeafPage = new LSHFLeafPage(headerPage.get_keyType());
        newLeafPageId = newLeafPage.getCurPage();

        // mapping
        if (DEBUG) {
          if (newLeafPageId.pid != -1) {
            setNodeType(newLeafPageId, NodeType.LEAF);
            System.out.println("Page " + newLeafPageId.pid + " is a leaf page.");
          } else {
            System.out.println("newLeafPageId is -1, unexpected.");
          }
        }

        newIndexPage.insertKey(new Vector100DKey(Integer.toString(i)), newLeafPageId, level); // insert <key, pid>

        // insert record to corresponding new leaf page (bucket)
        if (i == level) {
          upEntry = _insert(key, rid, newLeafPageId, level);
          if (upEntry == null) {
            inserted = true;
            System.out.println("[LSHFPrefixTree] Record inserted to leaf in split case.");
          }
        }

        // newBuckets[i] = newLeafPage;
        // newBucketPageIds[i] = newLeafPageId;

        unpinPage(newLeafPageId, true);

        if (DEBUG) {
          System.out.println("[LSHFPrefixTree] _insert(): X Created new leaf page " + newLeafPageId);
        }

        // doubts
        // newLeafPage.setNextPage(currentLeafPage.getNextPage());
        // newLeafPage.setPrevPage(currentLeafPageId);  // for dbl-linked list
        // currentLeafPage.setNextPage(newLeafPageId);
      }

      if (!inserted) {
        upEntry = new LSHFKeyDataEntry(Integer.toString(hashArray[level]), newIndexPageId); // ?
      } else {
        upEntry = null;
      }

      // newIndexPage.bucket_list = newBucketPageIds;  // Check later

      LSHFKeyDataEntry tmpEntry;
      RID firstRid = new RID();

      for (tmpEntry = currentLeafPage.getFirst(firstRid); tmpEntry != null; tmpEntry = currentLeafPage.getFirst(firstRid)) {
        _insert((Vector100DKey)tmpEntry.key, firstRid, newIndexPageId, level);
        // int bucketPageId = newIndexPage.getBucketPageId(hashArray[level]).pid;

        // if (DEBUG) {
        //   System.out.println("***** [LSHFPrefixTree] tmpEntry check *****");
        //   System.out.println("tmpEntry.key: " + ((Vector100DKey)(tmpEntry.key)).getKey());
        //   System.out.println("tmpEntry.data: " + ((LeafData)tmpEntry.data).getData());
        // }

        // newBuckets[idx].insertRecord(key, rid);
        currentLeafPage.deleteSortedRecord(firstRid);
      }

      // if (DEBUG) {
      //   System.out.println("***** [LSHFPrefixTree] key check *****");
      //   System.out.println("key: " + key.getKey());
      // }

      // _insert(key, rid, newIndexPageId, level);

      unpinPage(newIndexPageId, true);

      if (DEBUG && upEntry != null) {
        System.out.println("***** [LSHFPrefixTree] upEntry check *****");
        System.out.println("upEntry.key: " + ((Vector100DKey)(upEntry.key)).getKey());
        System.out.println("upEntry.data: " + ((IndexData)upEntry.data).getData());
      }
      unpinPage(currentLeafPageId);

      return upEntry;

    } else if (currentPage.getType() == NodeType.INDEX) {

      LSHFIndexPage currentIndexPage = new LSHFIndexPage(page, headerPage.get_keyType());
      PageId currentIndexPageId = currentPageId;
      PageId nextPageId;

      // mapping
      if (DEBUG) {
        if (currentIndexPageId.pid != -1) {
          setNodeType(currentIndexPageId, NodeType.INDEX);
          System.out.println("Page " + currentIndexPageId.pid + " is an index page.");
        } else {
          System.out.println("currentLeafPageId is -1, unexpected.");
        }
      }

      if (DEBUG) {
        System.out.println("***** [LSHFPrefixTree] Before Split *****");
        System.out.println("currentIndexPage: page " + currentIndexPageId);
        for (int i = 0; i < BUCKET_NUM; i++) {
          Vector100DKey tmpKey = new Vector100DKey(Integer.toString(i));
          PageId childPageId = currentIndexPage.getPageNoByKey(tmpKey);
          System.out.print("child page: page " + childPageId.pid + ", ");
          if (isLeafPage(childPageId)) {
            System.out.println("LEAF");
          } else {
            System.out.println("INDEX");
          }
        }
      }


      nextPageId = currentIndexPage.getPageNoByKey(new Vector100DKey(Integer.toString(hashArray[level])));  // get PID of corresponding key


      unpinPage(currentIndexPageId);

      if (DEBUG) {
        System.out.println("[LSHFPrefixTree] nextPageId: " + nextPageId);
      }

      upEntry = _insert(key, rid, nextPageId, level + 1);

      // two cases:
      // - upEntry == null: one level lower no split has occurred:
      //                     we are done.
      // - upEntry != null: one of the children has split and
      //                    upEntry is the new data entry which has
      //                    to be inserted on this index page
      if (upEntry == null) {
        return null;  // record inserted to leaf
      }

      currentIndexPage = new LSHFIndexPage(pinPage(currentPageId), headerPage.get_keyType());



      PageId oldLeafPageId = currentIndexPage.getPageNoByKey((Vector100DKey)(upEntry.key));

      LSHFLeafPage oldLeafPage = new LSHFLeafPage(pinPage(oldLeafPageId), headerPage.get_keyType());

      currentIndexPage.deleteSortedRecord(new RID(oldLeafPageId, (int)oldLeafPage.getSlotCnt() - 1));

      currentIndexPage.insertKey((Vector100DKey)(upEntry.key), ((IndexData)upEntry.data).getData(), level);

      // update mapping
      if (DEBUG) {
        if (oldLeafPageId.pid != -1) {
          removeNodeType(oldLeafPageId);
          System.out.println("Removing old leaf page: page " + oldLeafPageId.pid);
        } else {
          System.out.println("oldLeafPageId is -1, unexpected. Freeing an invalid page.");
        }
      }

      unpinPage(oldLeafPageId);
      freePage(oldLeafPageId);

      if (DEBUG) {
        System.out.println("***** [LSHFPrefixTree] After Split *****");
        System.out.println("currentIndexPage: page " + currentIndexPageId);
        for (int i = 0; i < BUCKET_NUM; i++) {
          Vector100DKey tmpKey = new Vector100DKey(Integer.toString(i));
          // System.out.println("child page: page " + currentIndexPage.getPageNoByKey(tmpKey).pid);
          PageId childPageId = currentIndexPage.getPageNoByKey(tmpKey);
          System.out.print("child page: page " + childPageId.pid + ", ");
          if (isLeafPage(childPageId)) {
            System.out.println("LEAF");
          } else {
            System.out.println("INDEX");
          }
        }
      }

      unpinPage(currentIndexPageId, true);
      try {
        insert(key, rid);
      } catch (Exception e) {
        System.out.println("insert error 1");
      }
      return null;

    }
    // LSHFKeyDataEntry tmpEntry;
    // RID firstRid = new RID();
    // for (tmpEntry = currentPageId.getFirst(firstRid)); tmpEntry != null; tmpEntry = ) {}


    // recursive case: reach leaf and leaf is full

    return null;
  }

  private int[] parseHashValue(String key) {
    int[] hashArray = new int[keySize];
    for (int i = 0; i < keySize; i++) {
      hashArray[i] = Integer.parseInt(key.split("_")[i]);
    }
    return hashArray;
  }

  private Page pinPage(PageId pageno) throws PinPageException {
    if (DEBUG) {
      System.out.println("[LSHFPrefixTree] pin page: " + pageno.pid);
    }
    try {
      Page page = new Page();
      SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
      return page;
    } catch (Exception e) {
      throw new PinPageException(e, "");
    }
  }

  private void unpinPage(PageId pageno) throws UnpinPageException {
    if (DEBUG) {
      System.out.println("[LSHFPrefixTree] unpin page: " + pageno.pid);
    }
    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
    } catch (Exception e) {
      throw new UnpinPageException(e, "");
    }
  }

  private void unpinPage(PageId pageno, boolean dirty) throws UnpinPageException {
    if (DEBUG) {
      System.out.println("[LSHFPrefixTree] unpin page: " + pageno.pid);
    }
    try {
      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
    } catch (Exception e) {
      throw new UnpinPageException(e, "");
    }
  }

  private void updateHeader(PageId newRoot)
  throws   IOException,
    PinPageException,
    UnpinPageException {
    LSHFPrefixTreeHeaderPage header;
    PageId old_data;

    header = new LSHFPrefixTreeHeaderPage(pinPage(headerPageId));
    old_data = headerPage.get_rootId();
    header.set_rootId(newRoot);
    unpinPage(headerPageId, true);
  }

  private PageId get_file_entry(String filename) throws GetFileEntryException {
    try {
      return SystemDefs.JavabaseDB.get_file_entry(filename);
    } catch (Exception e) {
      throw new GetFileEntryException(e, "");
    }
  }

  public void closePrefixTree() throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, IOException {
    if (headerPage != null) {
      SystemDefs.JavabaseBM.unpinPage(headerPage.getPageId(), true);
      headerPage = null;
    }
  }

  // Method to add a mapping
  public void setNodeType(PageId pageId, short type) {
    if (pageTypeMap.containsKey(pageId)) {
      System.out.println("Duplicate key found. The current value will not be inserted.");
    } else {
      pageTypeMap.put(pageId, type); // Insert new key-value pair
    }
  }

  // Method to retrieve the node type
  public short getNodeType(PageId pageId) {
    return pageTypeMap.getOrDefault(pageId, null); // Return null if not found
  }

  // Method to check if a page is a leaf node
  public boolean isLeafPage(PageId pageId) {
    return pageTypeMap.getOrDefault(pageId, NodeType.INDEX) == NodeType.LEAF;
  }

  // Method to remove a mapping when a page is freed
  public void removeNodeType(PageId pageId) {
    pageTypeMap.remove(pageId);
  }
}
