import bufmgr.BufMgr;
import dbmgr.DBOP;
import diskmgr.PCounter;
import global.*;
import heap.Heapfile;
import heap.Tuple;
import iterator.*;
import lshfindex.LSHFFileScan;
import lshfindex.LSHFIndexFile;
import lshfindex.Vector100DKey;
import btree.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static dbmgr.DBOP.loadAttrTypes;

public class query2 {

  public static void main(String[] args) {
    // Expecting: query DBNAME QSNAME INDEXOPTION NUMBUF
    if (args.length != 4) {
      System.err.println("Usage: query RELNAME1 RELNAME2 QSNAME NUMBUF");
      System.exit(1);
    }

    PCounter.initialize();
    String relName1 = args[0];
    String relName2 = args[1];
    String qsName = args[2];
    int numBuf = Integer.parseInt(args[3]);

    DBOP.open_databaseDBNAME("mydb", 1000, numBuf);

    // Initialize the buffer manager with NUMBUF pages.
    BufMgr bufMgr = new BufMgr(numBuf, null);

    try {
      QuerySpec[] qs_list = parseQuerySpec(qsName);
      QuerySpec qs = qs_list[0];
      QuerySpec qs2 = qs_list[1];


      Heapfile heapFile1 = new Heapfile(relName1);
      Heapfile heapFile2 = new Heapfile(relName2);

      AttrType[] attrTypes1 = null;
      AttrType[] attrTypes2 = null;
      short[] Ssizes = new short[1];
      Ssizes[0] = 30;
      short[] Rsizes = new short[1];
      Rsizes[0] = 15;

      attrTypes1 = loadAttrTypes(relName1);
      System.out.println("attrTypes1 check:");
      for (int i = 0; i < attrTypes1.length; i++) {
        System.out.println(attrTypes1[i]);
      }
      attrTypes2 = loadAttrTypes(relName2);
      System.out.println("attrTypes2 check:");
      for (int i = 0; i < attrTypes2.length; i++) {
        System.out.println(attrTypes2[i]);
      }

      FldSpec[] projlist = new FldSpec[qs.getOutputFields().length];
      RelSpec rel = new RelSpec(RelSpec.outer);
      for (int i = 0; i < qs.getOutputFields().length; i++)
        projlist[i] =
          new FldSpec(rel, i + 1);

      // Get output fields attributes types
      AttrType[] outAttrTypes = new AttrType[qs.getOutputFields().length];
      for (int i = 0; i < qs.getOutputFields().length; i++) {
        outAttrTypes[i] = attrTypes1[qs.getOutputFields()[i] - 1];
      }

      if (qs2.getQueryType() != null) {
        // DJOIN operation
        Vector100Dtype targetVector = readTargetVector(qs.getTargetFileName());

        // project of the first relation
        FldSpec[] proj_rel1 = new FldSpec[attrTypes1.length];
        for (int i = 0; i < attrTypes1.length; i++) {
          proj_rel1[i] = new FldSpec(
            new RelSpec(RelSpec.outer),
            i + 1
          );
        }

        // project of the joined output relation (first + second)
        FldSpec[] proj_join = new FldSpec[qs.getOutputFields().length + qs2.getOutputFields().length];
        for (int i = 0; i < qs.getOutputFields().length; i++) {
          proj_join[i] = new FldSpec(
            new RelSpec(RelSpec.outer),
            qs.getOutputFields()[i]
          );
        }
        for (int i = 0; i < qs2.getOutputFields().length; i++) {
          proj_join[i + qs.getOutputFields().length] = new FldSpec(
            new RelSpec(RelSpec.innerRel),
            qs2.getOutputFields()[i]
          );
        }

        AttrType[] joinAttrTypes = new AttrType[qs.getOutputFields().length + qs2.getOutputFields().length];
        for (int i = 0; i < qs.getOutputFields().length; i++) {
          joinAttrTypes[i] = attrTypes1[qs.getOutputFields()[i] - 1];
        }
        for (int i = 0; i < qs2.getOutputFields().length; i++) {
          joinAttrTypes[i + qs.getOutputFields().length] = attrTypes2[qs2.getOutputFields()[i] - 1];
        }

        if (qs2.getQueryType() == QueryType.RANGE) {
          // Range operation
          System.out.println("Performing DJOIN with RANGE operation...");
          if (qs.getUseIndex() && qs2.getUseIndex()) {
            // range HH
            System.out.println("Using index for DJOIN first query, using index for DJOIN second query...");

            //
            // 1) build and run the LSH range scan on rel1 to get outer tuples
            //
            int QA1 = qs.getQueryField();        // outer join column
            int QA2 = qs2.getQueryField();       // inner join column
            int D1  = qs2.getThreshold();    // range for rel1
            int D2  = qs2.getThreshold();     // join‐distance threshold

            // open rel1’s LSH index
            LSHFIndexFile idx1 = new LSHFIndexFile(relName1 + "_" + QA1);
            LSHFFileScan  scan1 = new LSHFFileScan(idx1, heapFile1, targetVector);
            // compute the starting bit‐string key for the rel1 range
            KeyClass startKey1 = new Vector100DKey(idx1.computeHash(targetVector, 0, idx1.getH()));
            // fetch all rel1 tuples within D1 of target
            Tuple[] outerTuples = scan1.LSHFFileRangeScan(
                                    startKey1, D1, attrTypes1, QA1);
            System.out.println("outer scan done. outer tuple number: " + outerTuples.length);

            // 2) for each outer tuple, probe rel2 by LSH range on its join‐vector
            System.out.println("DJOIN(range) HH Result Tuples:");
            for (Tuple t1 : outerTuples) {
              // extract the join‐vector from t1
              Vector100Dtype v1 = t1.get100DVectFld(QA1);
              // open rel2’s LSH index
              LSHFIndexFile idx2 = new LSHFIndexFile(relName2 + "_" + QA2);
              LSHFFileScan  scan2 = new LSHFFileScan(idx2, heapFile2, v1);
              KeyClass startKey2 = new Vector100DKey(idx2.computeHash(v1, 0, idx2.getH()));
              // fetch rel2 tuples whose vector is within D2 of v1
              System.out.println("start inner scan.");
              Tuple[] innerTuples = scan2.LSHFFileRangeScan(
                                      startKey2, D2, attrTypes2, QA2);

              // 3) join each matching inner tuple with t1 and output
              for (Tuple t2 : innerTuples) {
                Tuple out = new Tuple();
                out.setHdr((short)joinAttrTypes.length, joinAttrTypes, /* string sizes */ Ssizes);
                Projection.Join(
                  t1, attrTypes1,
                  t2, attrTypes2,
                  out, proj_join, proj_join.length);
                out.print(joinAttrTypes);
              }
            }
          } else if (qs.getUseIndex() && !qs2.getUseIndex()) {
            // range HN
            System.out.println("Using index for DJOIN first query, not using index for DJOIN second query...");
            //
            // 1) build and run the LSH range scan on rel1 to get outer tuples
            //
            int QA1 = qs.getQueryField();        // outer join column
            int QA2 = qs2.getQueryField();       // inner join column
            int D1  = qs.getThreshold();    // range for rel1
            int D2  = qs2.getThreshold();     // join‐distance threshold

            Iterator inlj;
            try {
              inlj = DistanceJoin.djoinRangeNLJ(relName1, attrTypes1, Ssizes, QA1, targetVector, D1, relName2, attrTypes2, Ssizes, QA2, D2, proj_join, /*n_out_flds*/joinAttrTypes.length, numBuf);
              Tuple resultTuple;
              System.out.println("DJOIN(range) HN Result Tuples:");
              while ((resultTuple = inlj.get_next()) != null) {
                resultTuple.print(joinAttrTypes);
              }
              inlj.close();
            } catch (Exception e) {
              e.printStackTrace();
              System.out.println("DJOIN using index failed.");
            }
          } else if (!qs.getUseIndex() && qs2.getUseIndex()) {
            // range NH
            System.out.println("Not using index for DJOIN first query, using index for DJOIN second query...");
            //
            // 1) build and run the LSH range scan on rel1 to get outer tuples
            //
            int QA2 = qs.getQueryField();        // outer join column
            int QA1 = qs2.getQueryField();       // inner join column
            int D2  = qs.getThreshold();    // range for rel1
            int D1  = qs2.getThreshold();     // join‐distance threshold

            Iterator inlj;

            FldSpec[] inverse_proj_join = new FldSpec[qs.getOutputFields().length + qs2.getOutputFields().length];
            for (int i = inverse_proj_join.length - 1, j = 0; i >= 0; i--, j++) {
              inverse_proj_join[j] = proj_join[i];
            }
            
            AttrType[] inverseJoinAttrTypes = new AttrType[qs.getOutputFields().length + qs2.getOutputFields().length];
            for (int i = inverseJoinAttrTypes.length - 1, j = 0; i >= 0; i--, j++) {
              inverseJoinAttrTypes[j] = joinAttrTypes[i];
            }

            try {
              inlj = DistanceJoin.djoinRangeNLJ(relName1, attrTypes1, Ssizes, QA1, targetVector, D1, relName2, attrTypes2, Ssizes, QA2, D2, inverse_proj_join, /*n_out_flds*/joinAttrTypes.length, numBuf);
              Tuple resultTuple;
              System.out.println("DJOIN(range) NH Result Tuples:");
              while ((resultTuple = inlj.get_next()) != null) {
                resultTuple.print(inverseJoinAttrTypes);
              }
              inlj.close();
            } catch (Exception e) {
              e.printStackTrace();
              System.out.println("DJOIN using index failed.");
            }

          } else {
            // range NN
            System.out.println("Not using index for DJOIN first query, not using index for DJOIN second query...");
            // Two FileScans: 1. Range query on first relation 2. Range query on second relation
            CondExpr[] outFilter1 = new CondExpr[2];
            outFilter1[0] = new CondExpr();
            outFilter1[0].next = null;
            outFilter1[0].op = new AttrOperator(AttrOperator.aopLE);
            outFilter1[0].type1 = new AttrType(AttrType.attrSymbol);
            outFilter1[0].type2 = new AttrType(AttrType.attrVector100D);
            outFilter1[0].operand1.symbol = new FldSpec(
              new RelSpec(RelSpec.outer),
              qs.getQueryField()
            );
            outFilter1[0].operand2.vector100D = targetVector;
            outFilter1[0].distance = qs.getThreshold();
            outFilter1[1] = null;

            CondExpr[] outFilter2 = new CondExpr[2];
            outFilter2[0] = new CondExpr();
            outFilter2[0].next = null;
            outFilter2[0].op = new AttrOperator(AttrOperator.aopLE);
            outFilter2[0].type1 = new AttrType(AttrType.attrSymbol);
            outFilter2[0].type2 = new AttrType(AttrType.attrSymbol);
            outFilter2[0].operand1.symbol = new FldSpec(  // the first relation is outer
              new RelSpec(RelSpec.outer),
              qs.getQueryField()
            );
            outFilter2[0].operand2.symbol = new FldSpec(  // the second relation is inner
              new RelSpec(RelSpec.innerRel),
              qs2.getQueryField()
            );
            outFilter2[0].distance = qs2.getThreshold();
            outFilter2[1] = null;

            FileScan am = null;
            try {
              am = new FileScan(
                relName1,
                attrTypes1,
                Ssizes,
                (short) attrTypes1.length,
                attrTypes1,
                attrTypes1.length,
                proj_rel1,
                outFilter1
              ); // Apply condition during scan
            } catch (Exception e) {
              e.printStackTrace();
            }

            NestedLoopsJoins inl = null;
            try {
              inl = new NestedLoopsJoins(attrTypes1, attrTypes1.length, Ssizes,
                                         attrTypes2, attrTypes2.length, Rsizes,
                                         10,
                                         am, relName2,
                                         outFilter2, null, proj_join, joinAttrTypes.length);
            } catch (Exception e) {
              System.err.println("*** Error preparing for nested_loop_join");
              System.err.println("" + e);
              e.printStackTrace();
              Runtime.getRuntime().exit(1);
            }

            Tuple resultTuple;
            System.out.println("DJOIN(range) NN Result Tuples:");
            while ((resultTuple = inl.get_next()) != null) {
              resultTuple.print(joinAttrTypes);
            }
            inl.close();
            am.close();
          }
        } else if (qs2.getQueryType() == QueryType.NN) {
          // Nearest Neighbor operation
          System.out.println("Performing DJOIN with NN operation...");
          if (qs.getUseIndex() && qs2.getUseIndex()) {
            // nn HH
            System.out.println("Using index for DJOIN query on first query, using index for DJOIN query on second query...");
            int QA1 = qs.getQueryField();
            int QA2 = qs2.getQueryField();
            int K1  = qs.getThreshold();      // top‐K on rel1
            int D2  = qs2.getThreshold(); // join threshold

            // Iterator inlj;
            // try {
            //   inlj = DistanceJoin.djoinNNINLJ(relName1, attrTypes1, Ssizes, QA1, targetVector, K1, relName2, attrTypes2, Ssizes, QA2, D2, proj_join, /*n_out_flds*/joinAttrTypes.length, numBuf);
            //   Tuple resultTuple;
            //   System.out.println("Result Tuple:");
            //   while ((resultTuple = inlj.get_next()) != null) {
            //     resultTuple.print(joinAttrTypes);
            //   }
            //   System.out.println("get_next done.");
            //   inlj.close();
            // } catch (Exception e) {
            //   e.printStackTrace();
            //   System.out.println("DJOIN using index failed.");
            // }

            // 1) get top‐K rel1 tuples nearest target
            LSHFIndexFile idx1 = new LSHFIndexFile(relName1 + "_" + QA1);
            LSHFFileScan  scan1 = new LSHFFileScan(idx1, heapFile1, targetVector);
            KeyClass startKey1 = new Vector100DKey(idx1.computeHash(targetVector, 0, idx1.getH()));
            Tuple[] outerTuples = scan1.LSHFFileNNScan(
                startKey1, K1, attrTypes1, QA1);

            // 2) for each outer, do LSH range on rel2
            for (Tuple t1 : outerTuples) {
              Vector100Dtype v1 = t1.get100DVectFld(QA1);
              LSHFIndexFile idx2 = new LSHFIndexFile(relName2 + "_" + QA2);
              LSHFFileScan  scan2 = new LSHFFileScan(idx2, heapFile2, v1);
              KeyClass startKey2 = new Vector100DKey(idx2.computeHash(v1, 0, idx2.getH()));
              Tuple[] innerTuples = scan2.LSHFFileRangeScan(
                  startKey2, D2, attrTypes2, QA2);
              // Tuple[] innerTuples = scan2.LSHFFileNNScan(
              //   startKey2, D2, attrTypes2, QA2);

              System.out.println("DJOIN(NN) HH Result Tuples:");
              for (Tuple t2 : innerTuples) {
                Tuple out = new Tuple();
                out.setHdr((short)joinAttrTypes.length, joinAttrTypes, Rsizes);
                Projection.Join(
                   t1, attrTypes1,
                   t2, attrTypes2,
                   out, proj_join, proj_join.length);
                out.print(joinAttrTypes);
              }
            }

          } else if (qs.getUseIndex() && !qs2.getUseIndex()) {
            // nn HN
            System.out.println("Using index for DJOIN query on first query, not using index for DJOIN query on second query...");
            int QA1 = qs.getQueryField();
            int QA2 = qs2.getQueryField();
            int K1  = qs.getThreshold();      // top‐K on rel1
            int D2  = qs2.getThreshold(); // join threshold

            Iterator inlj;
            try {
              inlj = DistanceJoin.djoinNNNLJ(relName1, attrTypes1, Ssizes, QA1, targetVector, K1, relName2, attrTypes2, Ssizes, QA2, D2, proj_join, /*n_out_flds*/joinAttrTypes.length, numBuf);
              Tuple resultTuple;
              System.out.println("DJOIN(NN) HN Result Tuples:");
              while ((resultTuple = inlj.get_next()) != null) {
                resultTuple.print(joinAttrTypes);
              }
              System.out.println("get_next done.");
              inlj.close();
            } catch (Exception e) {
              e.printStackTrace();
              System.out.println("DJOIN using index failed.");
            }
          } else if (!qs.getUseIndex() && qs2.getUseIndex()) {
            // nn NH
            // **File Scan and Sort for NN (using Sort iterator)**
            CondExpr[] outFilter = new CondExpr[2];
            outFilter[0] = new CondExpr();
            outFilter[0].next = null;
            outFilter[0].op = new AttrOperator(AttrOperator.aopLE);
            outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
            outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
            outFilter[0].operand1.symbol = new FldSpec(  // the first relation is outer
              new RelSpec(RelSpec.outer),
              qs.getQueryField()
            );
            outFilter[0].operand2.symbol = new FldSpec(  // the second relation is inner
              new RelSpec(RelSpec.innerRel),
              qs2.getQueryField()
            );
            outFilter[0].distance = qs2.getThreshold();
            outFilter[1] = null;

            TupleOrder[] order = new TupleOrder[2];
            order[0] = new TupleOrder(TupleOrder.Ascending);
            order[1] = new TupleOrder(TupleOrder.Descending);

            FileScan am = new FileScan(
              relName1,
              attrTypes1,
              Ssizes,
              (short) attrTypes1.length,
              attrTypes1,
              attrTypes1.length,
              proj_rel1,
              null
            );

            Sort sortIterator = new Sort(
              attrTypes1,
              (short) attrTypes1.length,
              Ssizes,
              am,
              qs.getQueryField(),
              order[0],
              32,
              500,
              targetVector,
              qs.getThreshold()
            );
            
            System.out.println("Not using index for DJOIN query on first query, using index for DJOIN query on second query...");
            // 1 for inner, 2 for outer
            int QA1 = qs.getQueryField();
            int QA2 = qs2.getQueryField();
            int K1  = qs.getThreshold();      // top‐K on rel1
            int D2  = qs2.getThreshold(); // join threshold

            String indexFileName = relName2 + "_" + QA2;
            LSHFIndexFile lshf = new LSHFIndexFile(indexFileName);
            LSHFFileScan scan = new LSHFFileScan(lshf, heapFile2, targetVector);
            String keyStr = lshf.computeHash(targetVector, 0, lshf.getH());
            Vector100DKey key = new Vector100DKey(keyStr);

            Tuple[] results = scan.LSHFFileRangeScan(key, D2, attrTypes2, QA2);

            Tuple outerTuple;
            System.out.println("DJOIN(NN) NH Result Tuples:");
            while ((outerTuple = sortIterator.get_next()) != null) {
              for (Tuple innerTuple : results) {
                Tuple out = new Tuple();
                out.setHdr((short)joinAttrTypes.length, joinAttrTypes, /* string sizes */ Ssizes);
                Projection.Join(
                  outerTuple, attrTypes1,
                  innerTuple, attrTypes2,
                  out, proj_join, proj_join.length);
                out.print(joinAttrTypes);
              }
            }
            
            sortIterator.close();
            am.close();

            // Iterator inlj;
            // FldSpec[] inverse_proj_join = new FldSpec[qs.getOutputFields().length + qs2.getOutputFields().length];
            // for (int i = inverse_proj_join.length - 1, j = 0; i >= 0; i--, j++) {
            //   inverse_proj_join[j] = proj_join[i];
            // }

            // try {
            //   inlj = DistanceJoin.djoinNNNLJ(relName2, attrTypes2, Ssizes, QA2, targetVector, D2, relName1, attrTypes1, Ssizes, QA1, K1, inverse_proj_join, /*n_out_flds*/joinAttrTypes.length, numBuf);
            //   Tuple resultTuple;
            //   System.out.println("Result Tuple:");
            //   while ((resultTuple = inlj.get_next()) != null) {
            //     resultTuple.print(joinAttrTypes);
            //   }
            //   System.out.println("get_next done.");
            //   inlj.close();
            // } catch (Exception e) {
            //   e.printStackTrace();
            //   System.out.println("DJOIN using index failed.");
            // }
          } else {
            // nn NN
            System.out.println("Not using index for DJOIN query on first query, not using index for DJOIN query on second query...");
            CondExpr[] outFilter = new CondExpr[2];
            outFilter[0] = new CondExpr();
            outFilter[0].next = null;
            outFilter[0].op = new AttrOperator(AttrOperator.aopLE);
            outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
            outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
            outFilter[0].operand1.symbol = new FldSpec(  // the first relation is outer
              new RelSpec(RelSpec.outer),
              qs.getQueryField()
            );
            outFilter[0].operand2.symbol = new FldSpec(  // the second relation is inner
              new RelSpec(RelSpec.innerRel),
              qs2.getQueryField()
            );
            outFilter[0].distance = qs2.getThreshold();
            outFilter[1] = null;

            TupleOrder[] order = new TupleOrder[2];
            order[0] = new TupleOrder(TupleOrder.Ascending);
            order[1] = new TupleOrder(TupleOrder.Descending);

            FileScan am = new FileScan(
              relName1,
              attrTypes1,
              Ssizes,
              (short) attrTypes1.length,
              attrTypes1,
              attrTypes1.length,
              proj_rel1,
              null
            );

            Sort sortIterator = new Sort(
              attrTypes1,
              (short) attrTypes1.length,
              Ssizes,
              am,
              qs.getQueryField(),
              order[0],
              32,
              500,
              targetVector,
              qs.getThreshold()
            );

            NestedLoopsJoins inl = null;
            try {
              inl = new NestedLoopsJoins(attrTypes1, attrTypes1.length, Ssizes,
                                         attrTypes2, attrTypes2.length, Rsizes,
                                         10,
                                         sortIterator, relName2,
                                         outFilter, null, proj_join, joinAttrTypes.length);
            } catch (Exception e) {
              System.err.println("*** Error preparing for nested_loop_join");
              System.err.println("" + e);
              e.printStackTrace();
              Runtime.getRuntime().exit(1);
            }

            Tuple resultTuple;
            System.out.println("DJOIN(NN) NN Result Tuples:");
            while ((resultTuple = inl.get_next()) != null) {
              resultTuple.print(joinAttrTypes);
            }
            inl.close();
            sortIterator.close();
            am.close();
          }
        }
      } else if (qs.getQueryType() == QueryType.SORT) {
        // Sort operation
        // Vector100Dtype targetVector = readTargetVector(qs.getTargetFileName());

        // FileScan fileScan = new FileScan(
        //   relName1,
        //   attrTypes1,
        //   Ssizes,
        //   (short) attrTypes1.length,
        //   outAttrTypes,
        //   qs.getOutputFields().length,
        //   projlist,
        //   null
        // );

        // TupleOrder[] order = new TupleOrder[2];
        // order[0] = new TupleOrder(TupleOrder.Ascending);
        // order[1] = new TupleOrder(TupleOrder.Descending);
        // Sort sortIterator = new Sort(
        //   outAttrTypes,
        //   (short) outAttrTypes.length,
        //   Ssizes,
        //   fileScan,
        //   qs.getQueryField(),
        //   order[0],
        //   32,
        //   500,
        //   targetVector,
        //   qs.getThreshold()
        // );
        // Tuple resultTuple;
        // System.out.println("Result Tuple:");
        // while ((resultTuple = sortIterator.get_next()) != null) {
        //   resultTuple.print(outAttrTypes);
        // }
        // sortIterator.close();
        // fileScan.close();

        System.out.println("Performing SORT operation...");

        // 1) scan the original file
        FileScan scan = new FileScan(
          relName1,
          attrTypes1, Ssizes, (short)attrTypes1.length,
          outAttrTypes, qs.getOutputFields().length,
          projlist,
          null
        );

        // 2) create a new heapfile for the sorted output
        String sortedName = relName1 + "_sorted";
        Heapfile sortedHF = new Heapfile(sortedName);

        // 3) build the Sort operator (external merge sort, uses at most numBuf pages)
        Sort sorter = new Sort(
          outAttrTypes, (short)outAttrTypes.length, Ssizes,
          scan,
          qs.getQueryField(),
          new TupleOrder(TupleOrder.Ascending),
          /*sortFldLen*/100,      // vector‐length
          /*n_pages*/ numBuf,
          readTargetVector(qs.getTargetFileName()),
          qs.getThreshold()
        );
        // Sort sortIterator = new Sort(
        //   outAttrTypes,
        //   (short) outAttrTypes.length,
        //   Ssizes,
        //   fileScan,
        //   qs.getQueryField(),
        //   order[0],
        //   32,
        //   500,
        //   targetVector,
        //   qs.getThreshold()
        // );

        // 4) pull sorted tuples out one by one and write them to sortedHF,
        //    unpinning each page as soon as we dirty it.
        Tuple t;
        while ((t = sorter.get_next()) != null) {
          t.setHdr((short)outAttrTypes.length, outAttrTypes, Ssizes);
          byte[] rec = t.getTupleByteArray();
          RID rid = sortedHF.insertRecord(rec);
          // immediately unpin the data page we just wrote to:
          SystemDefs.JavabaseBM.unpinPage(rid.pageNo, true);
        }

        // 5) clean up
        sorter.close();
        scan.close();
        // sortedHF.close();

        // 6) also unpin and flush the sorted file’s header page
        // PageId hdr = sortedHF.getHeaderPage().getCurPage();
        // SystemDefs.JavabaseBM.unpinPage(hdr, true);

        // 7) force all dirty pages to disk
        // SystemDefs.JavabaseBM.flushAllPages();

        System.out.println("Sorted data written to heapfile: " + sortedName);
      } else if (qs.getQueryType() == QueryType.FILTER) {
        // Filter operation
        System.out.println("Performing Filter operation...");

        if (qs.getUseIndex()) {
          System.out.println("Using index for filter query...");
          // which field to filter on?
          int fldno = qs.getQueryField();            // 1‑based field #
          int atype = attrTypes1[fldno - 1].attrType; // its type
          // read the literal value from the same file‑reading code you already have
          String fileName = qs.getTargetFileName();
          if (!fileName.endsWith(".txt")) fileName += ".txt";
          BufferedReader br = new BufferedReader(
            new FileReader("datafiles/phase3/" + fileName)
          );
          String token = br.readLine().trim().split("\\s+")[0];
          br.close();
          // build KeyClass for the index lookup
          KeyClass lowKey;
          switch (atype) {
          case AttrType.attrInteger:
            lowKey = new IntegerKey(Integer.parseInt(token)); break;
          case AttrType.attrReal:
            // B‑tree only supports integer keys, so cast
            lowKey = new IntegerKey((int)Float.parseFloat(token)); break;
          case AttrType.attrString:
            lowKey = new StringKey(token); break;
          default:
            throw new IllegalArgumentException("Unsupported filter type");
          }

          // open the B‑tree index on this column
          String idxName = relName1 + "_" + fldno;
          BTreeFile btf = new BTreeFile(idxName);    // open existing

          // do an equality scan: lo_key = hi_key = lowKey
          BTFileScan idxScan = (BTFileScan)btf.new_scan(lowKey, lowKey);

          // for each index entry, fetch the tuple from the heap and print
          KeyDataEntry entry;
          while ((entry = idxScan.get_next()) != null) {
            // get the RID out of the LeafData
            RID rid = ((LeafData)entry.data).getData();
            // fetch the tuple
            Tuple t = heapFile1.getRecord(rid);
            t.setHdr((short)attrTypes1.length, attrTypes1, Ssizes);
            t.print(attrTypes1);
          }
          idxScan.DestroyBTreeFileScan();

        } else {
          // **File Scan with Range Condition**

          // Check which attribute to filter
          int filterType = attrTypes1[qs.getQueryField() - 1].attrType;

          // Read the target from the file
          String fileName = qs.getTargetFileName();
          if (!fileName.endsWith(".txt")) fileName += ".txt";
          BufferedReader br = new BufferedReader(
            new FileReader("datafiles/phase3/" + fileName)
          );
          String line = br.readLine().trim();
          br.close();
          String token = line.split("\\s+")[0];

          CondExpr[] rangeCond = new CondExpr[2]; // For single condition, CondExpr[2] is usually enough, last one is null
          rangeCond[0] = new CondExpr();
          rangeCond[0].op = new AttrOperator(AttrOperator.aopEQ); // Less than or equal to range
          rangeCond[0].type1 = new AttrType(AttrType.attrSymbol);
          rangeCond[0].type2 = new AttrType(filterType);
          rangeCond[0].operand1.symbol = new FldSpec(
            new RelSpec(RelSpec.outer),
            qs.getQueryField()
          );
          switch (filterType) {
          case AttrType.attrInteger:
            rangeCond[0].operand2.integer = Integer.parseInt(token);
            break;
          case AttrType.attrReal:
            rangeCond[0].operand2.real = Float.parseFloat(token);
            break;
          case AttrType.attrString:
            rangeCond[0].operand2.string = token;
            break;
          default:
            throw new IllegalArgumentException("Unsupported attribute type for filtering.");
          }
//          rangeCond[0].operand2.vector100D = targetVector;
//          rangeCond[0].distance = 0;
          rangeCond[1] = null; // Terminator

          FileScan fileScan = new FileScan(
            relName1,
            attrTypes1,
            Ssizes,
            (short) attrTypes1.length,
            outAttrTypes,
            qs.getOutputFields().length,
            projlist,
            rangeCond
          ); // Apply condition during scan

          Tuple resultTuple;
          while ((resultTuple = fileScan.get_next()) != null) {
            resultTuple.print(attrTypes1);
          }
          fileScan.close();
        }
      } else if (qs.getQueryType() == QueryType.RANGE) {
        // Range operation
        System.out.println("Performing Range operation...");

        Vector100Dtype targetVector = readTargetVector(qs.getTargetFileName());

        if (qs.getUseIndex()) {
          System.out.println("Using index for range query...");
          // initialize the index file
          String indexFileName = relName1 + "_" + qs.getQueryField();
          LSHFIndexFile lshf = new LSHFIndexFile(indexFileName);
          LSHFFileScan scan = new LSHFFileScan(lshf, heapFile1, targetVector);
          String keyStr = lshf.computeHash(targetVector, 0, lshf.getH());
          Vector100DKey key = new Vector100DKey(keyStr);

          Tuple[] results = scan.LSHFFileRangeScan(key, qs.getThreshold(), attrTypes1, qs.getQueryField());
          System.out.println("Result Tuple:");
          for (Tuple tuple1 : results) {
            // project the output tuple
            Tuple Jtuple = new Tuple();
            AttrType[] Jtypes = new AttrType[outAttrTypes.length];
            TupleUtils.setup_op_tuple(Jtuple, Jtypes, attrTypes1, attrTypes1.length, outAttrTypes, Ssizes, projlist, outAttrTypes.length);
            tuple1.setHdr((short) attrTypes1.length, attrTypes1, Ssizes);
            Projection.Project(tuple1, outAttrTypes, Jtuple, projlist, outAttrTypes.length);

            Jtuple.print(attrTypes1);
          }
        } else {
          System.out.println("Not using index for range query...");

          // **File Scan with Range Condition**
          CondExpr[] rangeCond = new CondExpr[2]; // For single condition, CondExpr[2] is usually enough, last one is null
          rangeCond[0] = new CondExpr();
          rangeCond[0].op = new AttrOperator(AttrOperator.aopLE); // Less than or equal to range
          rangeCond[0].type1 = new AttrType(AttrType.attrSymbol);
          rangeCond[0].type2 = new AttrType(AttrType.attrVector100D);
          rangeCond[0].operand1.symbol = new FldSpec(
            new RelSpec(RelSpec.outer),
            qs.getQueryField()
          );
          rangeCond[0].operand2.vector100D = targetVector;
          rangeCond[0].distance = qs.getThreshold(); // Set target vector for distance calculation
          rangeCond[1] = null; // Terminator

          FileScan fileScan = new FileScan(
            relName1,
            attrTypes1,
            Ssizes,
            (short) attrTypes1.length,
            outAttrTypes,
            qs.getOutputFields().length,
            projlist,
            rangeCond
          ); // Apply condition during scan

          Tuple resultTuple;
          while ((resultTuple = fileScan.get_next()) != null) {
            resultTuple.print(attrTypes1);
          }
          fileScan.close();
        }
      } else if (qs.getQueryType() == QueryType.NN) {
        // Nearest Neighbor operation
        Vector100Dtype targetVector = readTargetVector(qs.getTargetFileName());

        if (qs.getUseIndex()) {
          System.out.println("Using index for NN query...");
          // initialize the index file
          String indexFileName = relName1 + "_" + qs.getQueryField();
          LSHFIndexFile lshf = new LSHFIndexFile(indexFileName);
          LSHFFileScan scan = new LSHFFileScan(lshf, heapFile1, targetVector);
          String keyStr = lshf.computeHash(targetVector, 0, lshf.getH());
          Vector100DKey key = new Vector100DKey(keyStr);

          Tuple[] results = scan.LSHFFileNNScan(key, qs.getThreshold(), attrTypes1, qs.getQueryField());
          System.out.println("Result Tuple:");
          for (Tuple tuple1 : results) {
            // project the output tuple
            Tuple Jtuple = new Tuple();
            AttrType[] Jtypes = new AttrType[outAttrTypes.length];
            TupleUtils.setup_op_tuple(Jtuple, Jtypes, attrTypes1, attrTypes1.length, outAttrTypes, Ssizes, projlist, outAttrTypes.length);
            tuple1.setHdr((short) attrTypes1.length, attrTypes1, Ssizes);
            Projection.Project(tuple1, outAttrTypes, Jtuple, projlist, outAttrTypes.length);

            Jtuple.print(attrTypes1);
          }
        } else {
          System.out.println("Using file scan for NN query...");
          // **File Scan and Sort for NN (using Sort iterator)**
          TupleOrder[] order = new TupleOrder[2];
          order[0] = new TupleOrder(TupleOrder.Ascending);
          order[1] = new TupleOrder(TupleOrder.Descending);

          FileScan fileScan = new FileScan(
            relName1,
            attrTypes1,
            Ssizes,
            (short) attrTypes1.length,
            outAttrTypes,
            qs.getOutputFields().length,
            projlist,
            null
          );

          Sort sortIterator = new Sort(
            outAttrTypes,
            (short) outAttrTypes.length,
            Ssizes,
            fileScan,
            qs.getQueryField(),
            order[0],
            32,
            500,
            targetVector,
            qs.getThreshold()
          );

          Tuple resultTuple;
          System.out.println("Result Tuple:");
          while ((resultTuple = sortIterator.get_next()) != null) {
            resultTuple.print(outAttrTypes);
          }
          sortIterator.close();
          fileScan.close();
        }
      }

      System.out.println("Page reads: " + PCounter.rcounter);
      System.out.println("Page writes: " + PCounter.wcounter);
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private static QuerySpec[] parseQuerySpec(String qsName) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(qsName));
    String line = br.readLine().trim();

    QuerySpec[] qs = new QuerySpec[2];
    qs[0] = new QuerySpec();
    qs[1] = new QuerySpec();

    if (line.startsWith("Sort(")) {
      qs[0].setQueryType(QueryType.SORT);
      String inside = line.substring("Sort(".length(), line.length() - 1);
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
      qs[0].setTargetFileName(tokens[1]); // T: target vector file name
      qs[0].setThreshold(Integer.parseInt(tokens[2])); // K
      int numOut = tokens.length - 3;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 3]);
      }
      qs[0].setOutputFields(outFields);
    } else if (line.startsWith("Filter(")) {
      qs[0].setQueryType(QueryType.FILTER);
      String inside = line.substring(
                        "Filter(".length(),
                        line.length() - 1
                      );
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
      qs[0].setTargetFileName(tokens[1]); // T: target vector file name
      qs[0].setUseIndex(tokens[2].equals("H")); // Use index or not
      int numOut = tokens.length - 3;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 3]);
      }
      qs[0].setOutputFields(outFields);
    } else if (line.startsWith("Range(")) {
      qs[0].setQueryType(QueryType.RANGE);
      String inside = line.substring(
                        "Range(".length(),
                        line.length() - 1
                      );
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
      qs[0].setTargetFileName(tokens[1]); // T: target vector file name
      qs[0].setThreshold(Integer.parseInt(tokens[2])); // D: distance threshold
      qs[0].setUseIndex(tokens[3].equals("H")); // Use index or not
      int numOut = tokens.length - 4;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 4]);
      }
      qs[0].setOutputFields(outFields);
    } else if (line.startsWith("NN(")) {
      qs[0].setQueryType(QueryType.NN);
      String inside = line.substring("NN(".length(), line.length() - 1);
      String[] tokens = inside.split(",");
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].trim();
      }
      qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
      qs[0].setTargetFileName(tokens[1]); // T: target vector file name
      qs[0].setThreshold(Integer.parseInt(tokens[2])); // K: number of nearest neighbors
      qs[0].setUseIndex(tokens[3].equals("H")); // Use index or not
      int numOut = tokens.length - 4;
      int[] outFields = new int[numOut];
      for (int i = 0; i < numOut; i++) {
        outFields[i] = Integer.parseInt(tokens[i + 4]);
      }
      qs[0].setOutputFields(outFields);
    } else if (line.startsWith("DJOIN(")) {
      // We have 2 more lines to read
      line = br.readLine().trim();
      if (line.startsWith("Range(")) {
        // First relation
        qs[0].setQueryType(QueryType.RANGE);
        String inside = line.substring("Range(".length(), line.length() - 2);
        String[] tokens = inside.split(",");
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = tokens[i].trim();
        }
        qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
        qs[0].setTargetFileName(tokens[1]); // T: target vector file name
        qs[0].setThreshold(Integer.parseInt(tokens[2])); // D: distance threshold
        qs[0].setUseIndex(tokens[3].equals("H")); // Use index or not
        int numOut = tokens.length - 4;
        int[] outFields = new int[numOut];
        for (int i = 0; i < numOut; i++) {
          outFields[i] = Integer.parseInt(tokens[i + 4]);
        }
        qs[0].setOutputFields(outFields);

        // Second relation
        line = br.readLine().trim();
        qs[1].setQueryType(QueryType.RANGE);
        inside = line.substring(0, line.length() - 2);
        tokens = inside.split(",");
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = tokens[i].trim();
        }
        qs[1].setQueryField(Integer.parseInt(tokens[0])); // QA
        qs[1].setThreshold(Integer.parseInt(tokens[1])); // D: distance threshold
        qs[1].setUseIndex(tokens[2].equals("H")); // Use index or not
        numOut = tokens.length - 3;
        outFields = new int[numOut];
        for (int i = 0; i < numOut; i++) {
          outFields[i] = Integer.parseInt(tokens[i + 3]);
        }
        qs[1].setOutputFields(outFields);

      } else if (line.startsWith("NN(")) {
        qs[0].setQueryType(QueryType.NN);
        String inside = line.substring("NN(".length(), line.length() - 2);
        String[] tokens = inside.split(",");
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = tokens[i].trim();
        }
        qs[0].setQueryField(Integer.parseInt(tokens[0])); // QA
        qs[0].setTargetFileName(tokens[1]); // T: target vector file name
        qs[0].setThreshold(Integer.parseInt(tokens[2])); // K: number of nearest neighbors
        qs[0].setUseIndex(tokens[3].equals("H")); // Use index or not
        int numOut = tokens.length - 4;
        int[] outFields = new int[numOut];
        for (int i = 0; i < numOut; i++) {
          outFields[i] = Integer.parseInt(tokens[i + 4]);
        }
        qs[0].setOutputFields(outFields);

        // Second relation
        line = br.readLine().trim();
        qs[1].setQueryType(QueryType.NN);
        inside = line.substring(0, line.length() - 2);
        tokens = inside.split(",");
        for (int i = 0; i < tokens.length; i++) {
          tokens[i] = tokens[i].trim();
        }
        qs[1].setQueryField(Integer.parseInt(tokens[0])); // QA
        qs[1].setThreshold(Integer.parseInt(tokens[1])); // K: number of nearest neighbors
        qs[1].setUseIndex(tokens[2].equals("H")); // Use index or not
        numOut = tokens.length - 3;
        outFields = new int[numOut];
        for (int i = 0; i < numOut; i++) {
          outFields[i] = Integer.parseInt(tokens[i + 3]);
        }
        qs[1].setOutputFields(outFields);

      } else {
        throw new IllegalArgumentException(
          "Invalid query specification format: " + line
        );
      }
    } else {
      throw new IllegalArgumentException(
        "Invalid query specification format: " + line
      );
    }
    br.readLine();

    return qs;
  }

  private static Vector100Dtype readTargetVector(String fileName)
  throws IOException {
    if (!fileName.endsWith(".txt")) fileName += ".txt";
    BufferedReader br = new BufferedReader(
      new FileReader("datafiles/phase3/" + fileName)
    );
    String line = br.readLine().trim();
    br.close();
    String[] tokens = line.split("\\s+");
    if (tokens.length != 100) {
      throw new IllegalArgumentException(
        "Target vector file must contain 100 integers."
      );
    }
    short[] vector = new short[100];
    for (int i = 0; i < 100; i++) {
      vector[i] = Short.parseShort(tokens[i]);
    }
    return new Vector100Dtype(vector);
  }
}