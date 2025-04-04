package lshfindex;

import java.io.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import heap.*;
import btree.*;

public class LSHF implements GlobalConst {

  private static boolean DEBUG = false;

  public final static int keyCompare(KeyClass key1, KeyClass key2)
    throws KeyNotMatchException {
      if (key1 instanceof Vector100DKey && key2 instanceof Vector100DKey) {
        return ((Vector100DKey) key1).getKey().compareTo(((Vector100DKey) key2).getKey());
      } else {
        throw new KeyNotMatchException(null, "Key types do not match");
      }
  }

  protected final static int getKeyLength(KeyClass key) 
    throws KeyNotMatchException, IOException {
      if (key instanceof Vector100DKey) {
        OutputStream out = new ByteArrayOutputStream();
        DataOutputStream outstr = new DataOutputStream(out);
        outstr.writeUTF(((Vector100DKey) key).getKey());
        return outstr.size();
      } else {
        throw new KeyNotMatchException(null, "Key type not supported");
      }
  }

  protected final static int getDataLength(short pageType) 
    throws NodeNotMatchException {
      if (pageType == NodeType.LEAF)
        return 8;
      else if (pageType == NodeType.INDEX)
        return 4;
      else
        throw new NodeNotMatchException(null, "Node types do not match");
  }

  protected final static int getKeyDataLength(KeyClass key, short pageType) 
    throws KeyNotMatchException, NodeNotMatchException, IOException {
      return getKeyLength(key) + getDataLength(pageType);
  }

  protected final static LSHFKeyDataEntry getEntryFromBytes(byte[] from, int offset, int length, int keyType, short nodeType)
    throws KeyNotMatchException, NodeNotMatchException, ConvertException {
      KeyClass key;
      DataClass data;
      int n;
      try {
        if (nodeType == NodeType.INDEX) {
          n = 4;
          data = new IndexData(Convert.getIntValue(offset + length - 4, from));
        } else if (nodeType == NodeType.LEAF) {
          n = 8;
          RID rid = new RID();
          rid.slotNo = Convert.getIntValue(offset + length - 8, from);
          rid.pageNo = new PageId();
          rid.pageNo.pid = Convert.getIntValue(offset + length - 4, from);
          data = new LeafData(rid);
        } else {
          throw new NodeNotMatchException(null, "Node types do not match");
        }

        if (keyType == AttrType.attrString) {
          key = new Vector100DKey(Convert.getStrValue(offset, from, length - n));

        } else {
          throw new KeyNotMatchException(null, "Key types do not match");
        }

        if (DEBUG) {
          System.out.println("[LSHF] getEntryFromBytes(): key = " + ((Vector100DKey)key).getKey());
        }

        return new LSHFKeyDataEntry(key, data);
      } catch (IOException e) {
        throw new ConvertException(e, "Conversion failed");
      }
  }

  protected final static byte[] getBytesFromEntry(LSHFKeyDataEntry entry) 
    throws KeyNotMatchException, NodeNotMatchException, ConvertException {
      byte[] data;
      int n, m;
      try {
        n = getKeyLength(entry.key);
        m = n;
        if (entry.data instanceof IndexData)
          n += 4;
        else if (entry.data instanceof LeafData)
          n += 8;

        data = new byte[n];

        if (entry.key instanceof Vector100DKey) {
          Convert.setStrValue(((Vector100DKey) entry.key).getKey(), 0, data);
        } else {
          throw new KeyNotMatchException(null, "Key types do not match");
        }

        if (entry.data instanceof IndexData) {
          Convert.setIntValue(((IndexData) entry.data).getData().pid, m, data);
        } else if (entry.data instanceof LeafData) {
          Convert.setIntValue(((LeafData) entry.data).getData().slotNo, m, data);
          Convert.setIntValue(((LeafData) entry.data).getData().pageNo.pid, m + 4, data);
        } else {
          throw new NodeNotMatchException(null, "Node types do not match");
        }
        return data;
      } catch (IOException e) {
        throw new ConvertException(e, "Conversion failed");
      }
  }

  // public static void printPrefixTree(LSHFPrefixTreeHeaderPage header) throws IOException, 
  //    ConstructPageException, 
  //    IteratorException,
  //    HashEntryNotFoundException,
  //    InvalidFrameNumberException,
  //    PageUnpinnedException,
  //    ReplacerException
  // {
  //     if(header.get_rootId().pid == INVALID_PAGE) {
  //       System.out.println("The Tree is Empty!!!");
  //       return;
  //     }
      
  //     System.out.println("");
  //     System.out.println("");
  //     System.out.println("");
  //     System.out.println("---------------The Prefix Tree Structure---------------");
      
      
  //     System.out.println(1+ "     "+header.get_rootId());
      
  //     _printTree(header.get_rootId(), "     ", 1, header.get_keyType());
      
  //     System.out.println("--------------- End ---------------");
  //     System.out.println("");
  //     System.out.println("");
  // }

  // private static void _printTree(PageId currentPageId, String prefix, int i, 
  //        int keyType) 
  //   throws IOException, 
  //    ConstructPageException, 
  //    IteratorException,
  //    HashEntryNotFoundException,
  //    InvalidFrameNumberException,
  //    PageUnpinnedException,
  //    ReplacerException
  //   {
  //     LSHFSortedPage sortedPage = new LSHFSortedPage(currentPageId, keyType);
  //     prefix=prefix+"       ";
  //     i++;
  //     if( sortedPage.getType()==NodeType.INDEX) {  
  //       LSHFIndexPage indexPage=new LSHFIndexPage((Page)sortedPage, keyType);
        
  //       // System.out.println(i+prefix+ indexPage.getPrevPage());
  //       _printTree( indexPage.getPrevPage(), prefix, i, keyType);
        
  //       RID rid=new RID();
  //       for( LSHFKeyDataEntry entry=indexPage.getFirst(rid); entry!=null; 
  //            entry=indexPage.getNext(rid)) {
  //         System.out.println(i+prefix+(IndexData)entry.data);
  //         _printTree( ((IndexData)entry.data).getData(), prefix, i, keyType);
  //       }
  //     }
  //     SystemDefs.JavabaseBM.unpinPage(currentPageId , true/*dirty*/);
  //   }
}
