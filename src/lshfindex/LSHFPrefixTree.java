package lshfindex;

import global.*;
import java.util.*;
import diskmgr.*;
import bufmgr.*;
import heap.*;
import btree.*;
import iterator.*;
import java.io.*;

/**
 * LSHFPrefixTree implements a prefix tree for the LSH-Forest.
 * It uses the btree package template: internal (index) pages hold <key, PageID> pairs
 * and leaf pages hold <key, RID> pairs. For simplicity, this implementation starts with
 * a single leaf page (i.e. no internal nodes) and does not implement full node splitting.
 *
 * The tree supports insertion, scanning all entries, range search, and nearest neighbor search.
 */
public class LSHFPrefixTree {
  
  // File name (base) for this prefix tree (used for header and pages)
  private String fileName;
  
  // Maximum key size (in our case, the number of hash values concatenated; equals h)
  private int keySize;
  
  // The key type: our hash keys are strings.
  private int keyType = AttrType.attrString;
  
  // The header page for the tree
  private LSHFPrefixTreeHeaderPage header;
  
  // The PageId of the root node.
  // For simplicity, if the tree is new, the root is a leaf page.
  private PageId rootId;
  
  // A constant for maximum records per leaf page. (In a full implementation, this
  // is determined by the page size.)
  private static final int MAX_RECORDS_PER_LEAF = 100;
  
  /**
   * Constructs a new LSHFPrefixTree.
   * This constructor creates a new tree with an empty header and no root.
   * @param fileName the base file name to be used for storing tree pages.
   * @param keySize the maximum key length (number of hash values to be concatenated).
   * @throws Exception if tree creation fails.
   */
  public LSHFPrefixTree(String fileName, int keySize) throws Exception {
    this.fileName = fileName;
    this.keySize = keySize;
    
    // Create a new header page.
    // In a full system, we would check if the file exists and open it; here we always create a new tree.
    header = new LSHFPrefixTreeHeaderPage();
    // Set an arbitrary magic number.
    header.set_magic0(0x1234);
    // Initially, there is no root.
    rootId = new PageId(-1);
    header.set_rootId(rootId);
    
    // For simplicity, immediately create a new leaf page to serve as the root.
    LSHFLeafPage leaf = new LSHFLeafPage(keyType);
    rootId = leaf.getCurPage();
    header.set_rootId(rootId);
    
    // In a full implementation, the header would be written to disk.
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
    // Open the root leaf page.
    LSHFLeafPage leaf = new LSHFLeafPage(rootId, keyType);
    // Insert the entry.
    leaf.insertRecord(key, rid);
    // Check for overflow.
    if (leaf.numberOfRecords() > MAX_RECORDS_PER_LEAF) {
      // Splitting logic should be implemented here.
      // For this simplified implementation, we simply ignore splitting.
      // In a full implementation, splitting the leaf and adjusting the tree structure is required.
    }
  }
  
  /**
   * Returns a list of all RIDs stored in the tree.
   * This is done by scanning the leaf pages.
   * @return a List of RIDs.
   * @throws Exception if scanning fails.
   */
  // public List<RID> scanAll() throws Exception {
  //   List<RID> result = new ArrayList<>();
    
  //   // For simplicity, assume a single leaf page.
  //   if (rootId.pid == -1)
  //     return result;
    
  //   LSHFLeafPage leaf = new LSHFLeafPage(rootId, keyType);
  //   RID rid = new RID();
  //   KeyDataEntry entry = leaf.getFirst(rid);
  //   while (entry != null) {
  //     // In our leaf page, entry.data holds the RID.
  //     result.add((RID) entry.data);
  //     entry = leaf.getNext(rid);
  //   }
  //   return result;
  // }
  
  /**
   * Performs a range search on the prefix tree.
   * Returns all RIDs whose key (hash value) is "close" to the query key.
   * In this simplified version, we use Hamming distance between the hash strings as a proxy
   * for similarity. The provided 'range' parameter is interpreted as a maximum allowed
   * Hamming distance.
   * @param queryKey the query key (of type Vector100DKey).
   * @param range the maximum allowed Hamming distance.
   * @return a List of RIDs matching the range criteria.
   * @throws Exception if the search fails.
   */
  // public List<RID> rangeSearch(Vector100DKey queryKey, double range) throws Exception {
  //   List<RID> result = new ArrayList<>();
  //   String q = queryKey.toString();
    
  //   // For simplicity, scan only the root leaf page.
  //   if (rootId.pid == -1)
  //     return result;
    
  //   LSHFLeafPage leaf = new LSHFLeafPage(rootId, keyType);
  //   RID rid = new RID();
  //   KeyDataEntry entry = leaf.getFirst(rid);
  //   while (entry != null) {
  //     String keyStr = entry.key.toString();
  //     int dist = hammingDistance(q, keyStr);
  //     if (dist <= range) {
  //       result.add((RID) entry.data);
  //     }
  //     entry = leaf.getNext(rid);
  //   }
  //   return result;
  // }
  
  /**
   * Performs a nearest neighbor search on the prefix tree.
   * Returns the top 'count' candidate RIDs whose keys are closest to the query key.
   * Closeness is measured by Hamming distance between the hash strings.
   * @param queryKey the query key (of type Vector100DKey).
   * @param count the number of nearest neighbors to return.
   * @return a List of candidate RIDs.
   * @throws Exception if the search fails.
   */
  // public List<RID> nnSearch(Vector100DKey queryKey, int count) throws Exception {
  //   List<RID> result = new ArrayList<>();
  //   List<Candidate> candidates = new ArrayList<>();
  //   String q = queryKey.toString();
    
  //   // For simplicity, scan only the root leaf page.
  //   if (rootId.pid == -1)
  //     return result;
    
  //   LSHFLeafPage leaf = new LSHFLeafPage(rootId, keyType);
  //   RID rid = new RID();
  //   KeyDataEntry entry = leaf.getFirst(rid);
  //   while (entry != null) {
  //     String keyStr = entry.key.toString();
  //     int dist = hammingDistance(q, keyStr);
  //     candidates.add(new Candidate((RID) entry.data, dist));
  //     entry = leaf.getNext(rid);
  //   }
    
  //   // Sort candidates by Hamming distance.
  //   Collections.sort(candidates, new Comparator<Candidate>() {
  //     public int compare(Candidate a, Candidate b) {
  //       return Integer.compare(a.distance, b.distance);
  //     }
  //   });
    
  //   for (int i = 0; i < Math.min(count, candidates.size()); i++) {
  //     result.add(candidates.get(i).rid);
  //   }
    
  //   return result;
  // }
  
  /**
   * Computes the Hamming distance between two strings.
   */
  private int hammingDistance(String s1, String s2) {
    int dist = 0;
    int len = Math.min(s1.length(), s2.length());
    for (int i = 0; i < len; i++) {
      if (s1.charAt(i) != s2.charAt(i))
        dist++;
    }
    dist += Math.abs(s1.length() - s2.length());
    return dist;
  }
  
  /**
   * A helper class to hold candidate entries for nearest neighbor search.
   */
  private class Candidate {
    RID rid;
    int distance;
    
    public Candidate(RID rid, int distance) {
      this.rid = rid;
      this.distance = distance;
    }
  }

    public void closePrefixTree() throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException, IOException
  {
    if ( header!=null) {
      SystemDefs.JavabaseBM.unpinPage(header.getPageId(), true);
      header=null;
    }  
  }
}
