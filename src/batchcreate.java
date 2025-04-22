import static dbmgr.DBOP.create_tuple;
import static dbmgr.DBOP.save_attrTypes;

import dbmgr.DBOP;
import diskmgr.PCounter;
import global.AttrType;
import global.RID;
import global.Vector100Dtype;
import heap.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class batchcreate {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: batchcreate <DATAFILENAME> <RELNAME>");
            System.exit(1);
        }

        PCounter.initialize();

        String dataFilename = args[0];
        String relName = args[1];

        DBOP.open_databaseDBNAME("mydb", 1000, 4000);
        DBOP.cleanup("mydb");

        try {
            BufferedReader br = new BufferedReader(
                new FileReader(dataFilename)
            );
            int numAttrs = Integer.parseInt(br.readLine().trim());
            String[] typeTokens = br.readLine().trim().split("\\s+");

            AttrType[] attrTypes = new AttrType[numAttrs];
            for (int i = 0; i < numAttrs; i++) {
                int typeCode = Integer.parseInt(typeTokens[i]);
                switch (typeCode) {
                    case 1:
                        attrTypes[i] = new AttrType(AttrType.attrInteger);
                        break;
                    case 2:
                        attrTypes[i] = new AttrType(AttrType.attrReal);
                        break;
                    case 3:
                        attrTypes[i] = new AttrType(AttrType.attrString);
                        break;
                    case 4:
                        attrTypes[i] = new AttrType(AttrType.attrVector100D);
                        break;
                    default:
                        throw new IOException(
                            "Unknown attribute type code: " + typeCode
                        );
                }
            }

            save_attrTypes(relName, numAttrs, attrTypes);

            // Create the heap file for storing tuples (database file)
            Heapfile hf = new Heapfile(relName);

            // Process tuples.
            // Each tuple is stored in the file as n consecutive lines.
            String line;
            boolean eof = false;
            while ((line = br.readLine()) != null) {
                // Read one tuple; first attribute is already in 'line'
                String[] tupleValues = new String[numAttrs];
                tupleValues[0] = line;
                for (int i = 1; i < numAttrs; i++) {
                    String nextLine = br.readLine();
                    if (nextLine != null) {
                        //            String truncated = nextLine.substring(0, 50);
                        //            System.out.println("nextLine: " + truncated + "...");
                    }
                    if (nextLine == null) {
                        eof = true;
                        System.err.println("End of file.");
                        break;
                    }
                    tupleValues[i] = nextLine;
                }
                if (eof) break;

                // Create a tuple with numAttrs fields.
                Tuple tuple = create_tuple(numAttrs, attrTypes, tupleValues);

                // Convert the tuple to a byte array and insert it into the heap file.
                byte[] tupleData = tuple.getTupleByteArray();
                RID rid = hf.insertRecord(tupleData);
                System.out.printf(
                    "Inserted tuple with RID<%d, %d>\n",
                    rid.pageNo.pid,
                    rid.slotNo
                );
            }
            DBOP.close_database();

            System.out.println(
                "Heapfile contains " + hf.getRecCnt() + " tuples"
            );
            // At the end of batch insertion, output the disk I/O counts.
            System.out.println("Page reads: " + PCounter.rcounter);
            System.out.println("Page writes: " + PCounter.wcounter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
