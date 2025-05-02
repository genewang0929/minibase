import dbmgr.DBOP;
import java.io.*;
import global.*;
import heap.*;
import btree.*;
import lshfindex.*;
import diskmgr.*;
import bufmgr.*;

public class createindex {

  private static boolean DEBUG = true;

  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Usage: createindex RELNAME COLUMNID L h");
      System.exit(1);
    }
    String relName   = args[0];
    int    colNo     = Integer.parseInt(args[1]); // 1-based
    int    L         = Integer.parseInt(args[2]);
    int    h         = Integer.parseInt(args[3]);

    try {
      // initialize Minibase on existing DB
      DBOP.open_databaseDBNAME("mydb", 1000, 1000);

      // reset I/O counters
      PCounter.initialize();

      // load the schema file we previously wrote for this heap
      AttrType[] types = DBOP.loadAttrTypes(relName);
      // build a default strSizes array: for every string attribute assume length 30
      short[]    strSizes = new short[types.length];
      for (int i = 0; i < types.length; i++)
        if (types[i].attrType == AttrType.attrString)
          strSizes[i] = 30;
        else
          strSizes[i] = 0;

      // open the heap file for this relation
      Heapfile hf = new Heapfile(relName);

      // build the index‐file name
      String idxName = relName + "_" + colNo;

      if (DEBUG) {
        System.out.println("***********************");
        System.out.println("[Index File Name]: " + idxName);
        System.out.println("***********************");
      }


      // decide vector vs scalar
      if (types[colNo-1].attrType == AttrType.attrVector100D) {
        // --- build LSH-forest index ---
        LSHFIndexFile lshIndex = new LSHFIndexFile(idxName, h, L, types.length, types);

        Scan scan = hf.openScan();
        RID rid = new RID();
        Tuple t;
        while ((t = scan.getNext(rid)) != null) {
          t.setHdr((short)types.length, types, strSizes);
          Vector100Dtype v = t.get100DVectFld(colNo);
          lshIndex.insert(v, rid);
        }
        scan.closescan();
        lshIndex.close();
      }
      else {
        int keyType;
        if (types[colNo-1].attrType == AttrType.attrInteger || types[colNo-1].attrType == AttrType.attrReal) {
          keyType = AttrType.attrInteger;
        } else {
          keyType = AttrType.attrString;
        }
        // --- build B-tree index ---
        BTreeFile btf = new BTreeFile(
            idxName,
            keyType,
            /* keysize */ 4,
            /* delete fashion */ 1);

        Scan scan = hf.openScan();
        RID rid = new RID();
        Tuple t;
        while ((t = scan.getNext(rid)) != null) {
          t.setHdr((short)types.length, types, strSizes);
          KeyClass key;
          switch(types[colNo-1].attrType) {
            case AttrType.attrInteger:
              key = new IntegerKey(t.getIntFld(colNo)); break;
            case AttrType.attrReal:
              key = new IntegerKey((int)t.getFloFld(colNo)); break;
            case AttrType.attrString:
              key = new StringKey(t.getStrFld(colNo)); break;
            default:
              throw new IllegalArgumentException("Unsupported type");
          }
          btf.insert(key, rid);
        }
        btf.close();
        scan.closescan();
      }

      // flush and report I/O
      SystemDefs.JavabaseBM.flushAllPages();
      System.out.println("Page reads:  " + PCounter.rcounter);
      System.out.println("Page writes: " + PCounter.wcounter);
    }
    catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
