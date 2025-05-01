package iterator;

import global.*;
import heap.*;
import btree.*;
import lshfindex.*;
import lshfindex.LSHFIndexFile;

import java.io.*;
import java.util.*;

/**
 * A little Iterator that returns the contents of a Tuple[].
 */
class TupleArrayIterator extends Iterator {
  private Tuple[] _tuples;
  private int      _pos;
  public TupleArrayIterator(AttrType[] types, short[] strSizes, Tuple[] tuples)
      throws Exception {
    _tuples = tuples;
    _pos = 0;
    for (Tuple t : _tuples) {
      t.setHdr((short)types.length, types, strSizes);
    }
  }
  public Tuple get_next() { 
    if (_pos >= _tuples.length) return null;
    return _tuples[_pos++];
  }
  public void close() {}
}

/**
 * Implements the two “distance‐join” queries DJOIN₁ (range‐join) and DJOIN₂ (kNN‐join).
 */
public class DistanceJoin {

  private static final boolean DEBUG = true;

  /**
   * DJOIN1: join Range‐query(R1) with R2 by distance ≤ D2 on (QA1,QA2).
   */
  public static Iterator djoinRange(
      String rel1, AttrType[] type1, short[] ss1,
      int QA1, Vector100Dtype T1, int D1,
      String rel2, AttrType[] type2, short[] ss2,
      int QA2, int D2,
      FldSpec[] proj_list, int n_out_flds,
      int amt_of_mem
  ) throws Exception {

    // 1) LSH‐range‐scan on R1
    Heapfile hf1 = new Heapfile(rel1);
    LSHFIndexFile lshIndex1 = new LSHFIndexFile(rel1 + "_" + QA1);
    LSHFFileScan scan1 = new LSHFFileScan(lshIndex1, hf1, T1);
    String bit = lshIndex1.computeHash(T1, /*layer*/0, /*prefix*/lshIndex1.getH());
    KeyClass startKey = new Vector100DKey(bit);
    Tuple[] outerTuples = scan1.LSHFFileRangeScan(startKey, D2, type1, QA1);

    // 2) wrap as Iterator
    TupleArrayIterator outerIter = new TupleArrayIterator(type1, ss1, outerTuples);

    // 3) build join condition
    CondExpr[] outFilter   = null;           // no extra outer filter
    CondExpr[] rightFilter = new CondExpr[1];
    CondExpr ce = new CondExpr();
    ce.op        = new AttrOperator(AttrOperator.aopLE);
    ce.type1     = new AttrType(AttrType.attrVector100D);
    ce.type2     = new AttrType(AttrType.attrVector100D);

    ce.operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), QA2);
    ce.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),   QA1);
    ce.distance = D2;
    rightFilter[0] = ce;

    // 4) pick index type
    IndexType idxType = new IndexType(IndexType.LSHFIndex);
    String idxName = rel1 + "_" + QA1;

    if (DEBUG) {
      System.out.println("[DistanceJoin] ready to return.");
    }

    // 5) return an Index‐Nested‐Loop Join
    return new INLJoins(
      type1, type1.length, ss1,
      type2, type2.length, ss2,
      amt_of_mem,
      outerIter,
      rel2,
      idxType,
      idxName,
      outFilter,
      rightFilter,
      proj_list,
      n_out_flds
    );
  }

  /**
   * DJOIN2: join kNN‐query(R1) with R2 by distance ≤ D2 on (QA1,QA2).
   */
  public static Iterator djoinNN(
      String rel1, AttrType[] type1, short[] ss1,
      int QA1, Vector100Dtype T1, int K1,
      String rel2, AttrType[] type2, short[] ss2,
      int QA2, int D2,
      FldSpec[] proj_list, int n_out_flds,
      int amt_of_mem
  ) throws Exception {

    // 1) LSH‐kNN‐scan on R1
    Heapfile hf1 = new Heapfile(rel1);
    LSHFIndexFile lshIndex1 = new LSHFIndexFile(rel1 + "_" + QA1);
    LSHFFileScan scan1 = new LSHFFileScan(lshIndex1, hf1, T1);
    String bit = lshIndex1.computeHash(T1, /*layer*/0, /*prefix*/lshIndex1.getH());
    KeyClass startKey = new Vector100DKey(bit);
    Tuple[] outerTuples = scan1.LSHFFileNNScan(startKey, K1, type1, QA1);

    // 2) wrap as Iterator
    TupleArrayIterator outerIter = new TupleArrayIterator(type1, ss1, outerTuples);

    // 3) build join condition
    CondExpr[] outFilter   = null;
    CondExpr[] rightFilter = new CondExpr[1];
    CondExpr ce = new CondExpr();
    ce.op        = new AttrOperator(AttrOperator.aopLE);
    ce.type1     = new AttrType(AttrType.attrVector100D);
    ce.type2     = new AttrType(AttrType.attrVector100D);
    ce.operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), QA2);
    ce.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),   QA1);
    ce.distance = D2;
    rightFilter[0] = ce;

    IndexType idxType = new IndexType(IndexType.B_Index);
    String idxName   = rel1 + "_" + QA1;

    if (DEBUG) {
      System.out.println("[DistanceJoin] ready to return.");
    }

    return new INLJoins(
      type1, type1.length, ss1,
      type2, type2.length, ss2,
      amt_of_mem,
      outerIter,
      rel2,
      idxType,
      idxName,
      outFilter,
      rightFilter,
      proj_list,
      n_out_flds
    );
  }
}
