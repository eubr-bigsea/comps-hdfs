package test;

import integration.Block;
import integration.HDFS;


import java.util.ArrayList;
import java.lang.*;


/**
 * Created by lucasmsp on 16/05/16.
 */


public class HDFSTest_LineReader {

    static String defaultFS = System.getenv("MASTER_HADOOP_URL"); // local of the HDFS's master node

    public static void main(String args[]) {
        long startTime;
        double seconds;
        String first = "";
        String last = "";
        int frag = 2;
        int n_blk = 0;
        ArrayList<Block> HDFS_SPLITS_LIST;
        String name;

        boolean test_getRecords = false;
        boolean test2 = false;
        boolean test_chunck = true;
        try {
             ArrayList<String> table_comparable1 = new ArrayList<String>();
             ArrayList<String> table_comparable2 = new ArrayList<String>();
             System.out.println("DefaultFS:" + defaultFS);
             HDFS dfs =  new HDFS(defaultFS);
             String[] all;

            if(test_getRecords) {
                System.out.println("\n\n#########\nTesting:  dfs.findBlocksByRecords\n#########");

                name = "/user/pdm2/FILE_TEST_01.txt"; // each line has same size
                System.out.println("Reading file:" + name + " \n");
                HDFS_SPLITS_LIST = dfs.findBlocksByRecords(name, frag);
                n_blk = HDFS_SPLITS_LIST.size();
                System.out.println("Number of HDFS BLOCKS: " + n_blk);


                System.out.println("Testing getRecord() and HasRecord()");
                HDFS_SPLITS_LIST = dfs.findBlocksByRecords(name, frag);
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                    int num_lines = 0;
                    System.out.println(b.toString());
                    first = b.getRecord();
                    num_lines++;
                    while (b.HasRecords()) {
                        last = b.getRecord();
                        num_lines++;
                    }
                    b.closeBlock();
                    table_comparable1.add(first);
                    table_comparable1.add(last);
                    System.out.println("First:" + first + "\t\tLast:" + last + "\t\tLength:" + num_lines);
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n", seconds);

                System.out.println("Testing getRecords()");
                //HDFS_SPLITS_LIST = dfs.findBlocksByRecords(name, frag);
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                    b.restart();
                    System.out.println(b.toString());
                    all = b.getRecords();
                    first = all[0];
                    last = all[all.length - 1];
                    b.closeBlock();
                    System.out.println("First:" + first + "\t\tLast:" + last + "\t\tLength:" + all.length);
                    table_comparable2.add(first);
                    table_comparable2.add(last);
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n", seconds);

                name = "/user/pdm2/FILE_TEST_02.txt"; // each line has same size
                System.out.println("Reading file:" + name + " \n");
                HDFS_SPLITS_LIST = dfs.findBlocksByRecords(name, 2);
                n_blk = HDFS_SPLITS_LIST.size();
                System.out.println("Number of HDFS BLOCKS: " + n_blk);


                System.out.println("Testing getRecord() and HasRecord()");
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                    int num_lines = 0;
                    System.out.println(b.toString());
                    first = b.getRecord();
                    num_lines++;
                    while (b.HasRecords()) {
                        last = b.getRecord();
                        num_lines++;
                    }
                    b.closeBlock();
                    table_comparable1.add(first);
                    table_comparable1.add(last);
                    System.out.println("First:" + first + "\t\tLast:" + last + "\t\tLength:" + num_lines);
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n", seconds);


                System.out.println("Testing getRecords()");
                //HDFS_SPLITS_LIST = dfs.findBlocksByRecords(name, 2);
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                    b.restart();
                    System.out.println(b.toString());
                    all = b.getRecords();
                    first = all[0];
                    last = all[all.length - 1];
                    b.closeBlock();
                    table_comparable2.add(first);
                    table_comparable2.add(last);
                    System.out.println("First:" + first + "\t\tLast:" + last + "\t\tLength:" + all.length);
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n\n", seconds);

                System.out.println("\n\n#########\nTesting:  dfs.findALLBlocks\n#########");

                name = "/user/pdm2/FILE_TEST_01.txt"; // each line has same size
                System.out.println("Reading file:" +name+" \n");
                HDFS_SPLITS_LIST = dfs.findALLBlocks(name);
                n_blk = HDFS_SPLITS_LIST.size();
                System.out.println("Number of HDFS BLOCKS: " + n_blk);

                System.out.println("Testing getRecord() and HasRecord()");
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                     int num_lines = 0;
                     System.out.println(b.toString());
                     first = b.getRecord();
                     num_lines++;
                     while (b.HasRecords()) {
                         last = b.getRecord();
                         num_lines++;
                     }
                     b.closeBlock();
                     table_comparable1.add(first);
                     table_comparable1.add(last);
                     System.out.println("First:"+first + "\t\tLast:"+last + "\t\tLength:" + num_lines );
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n",seconds);

                System.out.println("Testing getRecords()");
                //HDFS_SPLITS_LIST = dfs.findALLBlocks(name);
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                     b.restart();
                     System.out.println(b.toString());
                     all = b.getRecords();
                     first = all[0];
                     last = all[all.length-1];
                     b.closeBlock();
                     table_comparable2.add(first);
                     table_comparable2.add(last);
                     System.out.println("First:"+first + "\t\tLast:"+last + "\t\tLength:" + all.length );
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n",seconds);

                 name = "/user/pdm2/FILE_TEST_02.txt"; // each line has same size
                 System.out.println("Reading file:" +name+" \n");
                 HDFS_SPLITS_LIST = dfs.findALLBlocks(name);
                 n_blk = HDFS_SPLITS_LIST.size();
                 System.out.println("Number of HDFS BLOCKS: " + n_blk);


                 System.out.println("Testing getRecord() and HasRecord()");
                 startTime = System.nanoTime();
                 for (Block b : HDFS_SPLITS_LIST) {
                     int num_lines = 0;
                     System.out.println(b.toString());
                     first = b.getRecord();
                     num_lines++;
                     while (b.HasRecords()) {
                         last = b.getRecord();
                         num_lines++;
                     }
                     b.closeBlock();
                     table_comparable1.add(first);
                     table_comparable1.add(last);
                     System.out.println("First:"+first + "\t\tLast:"+last + "\t\tLength:" + num_lines );
                 }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n",seconds);

                System.out.println("Testing getRecords()");
                //HDFS_SPLITS_LIST = dfs.findALLBlocks(name);
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                     b.restart();
                     System.out.println(b.toString());
                     all = b.getRecords();
                     first = all[0];
                     last = all[all.length-1];
                     b.closeBlock();
                     table_comparable2.add(first);
                     table_comparable2.add(last);
                     System.out.println("First:"+first + "\t\tLast:"+last + "\t\tLength:" + all.length );
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n",seconds);





                //----------------------------------------------------------------------------------------
                // INFORMATIONS
                //----------------------------------------------------------------------------------------

                int size = table_comparable1.size() ;
                System.out.println("Tests: "+size);
                for (int i = 0; i< size;i++){
                    String ok = "[ERROR]";
                    if(table_comparable1.get(i).equals(table_comparable2.get(i)))
                        ok = " [OK!]";
                    System.out.println(table_comparable1.get(i) + " "+ table_comparable2.get(i) +"  "+ ok);
                }

            }

            if(test2) {

                ///----------------------------------------------------------------------------------------

                System.out.println("\n\n#############################\n\tTesting:  Dataset's file\n#########");


                System.out.println("\n\n#########\nTesting:  dfs.findALLBlocks\n#########");


                name = "/user/pdm2/train_0.0012m.csv"; // each line has same size
                System.out.println("Reading file:" + name + " \n");
                HDFS_SPLITS_LIST = dfs.findALLBlocks(name);
                n_blk = HDFS_SPLITS_LIST.size();
                System.out.println("Number of HDFS BLOCKS: " + n_blk);
                float[] line = new float[29];
                String[] lines;
                System.out.println("Testing getRecord() and HasRecord()");
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                    int num_lines = 0;
                    System.out.println(b.toString());
                    while (b.HasRecords()) {

                        first = b.getRecord();
                        lines = first.split(",");
                        if (lines.length != 29) {
                            System.out.println("[ERROR] - num features:" + lines.length);
                            System.out.println(first);
                        }
                        for (int i = 0; i < lines.length; i++)
                            line[i] = Float.parseFloat(lines[i]);
                        num_lines++;
                    }

                    System.out.println("Length:" + num_lines);
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n", seconds);

                System.out.println("Testing getRecords()");
                HDFS_SPLITS_LIST = dfs.findALLBlocks(name);
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                    System.out.println(b.toString());
                    all = b.getRecords();

                    first = all[0];
                    last = all[all.length - 1];


                    for (int i = 0; i < all.length; i++) {
                        lines = all[i].split(",");
                        if (lines.length != 29) {
                            System.out.println("[ERROR] - num features:" + lines.length);
                            System.out.println(first);
                        }
                        for (int j = 0; j < lines.length; j++)
                            line[j] = Float.parseFloat(lines[j]);
                    }
                    System.out.println("Length:" + all.length);
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n", seconds);


                System.out.println("\n\n#########\nTesting:  dfs.findBlocksByRecords\n#########");

                name = "/user/pdm2/train_0.0012m.csv"; // each line has same size
                System.out.println("Reading file:" + name + " \n");
                HDFS_SPLITS_LIST = dfs.findBlocksByRecords(name, 4);
                n_blk = HDFS_SPLITS_LIST.size();
                System.out.println("Number of HDFS BLOCKS: " + n_blk);

                System.out.println("Testing getRecord() and HasRecord()");
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                    int num_lines = 0;
                    System.out.println(b.toString());
                    while (b.HasRecords()) {

                        first = b.getRecord();
                        lines = first.split(",");
                        if (lines.length != 29) {
                            System.out.println("[ERROR] - num features:" + lines.length);
                            System.out.println(first);
                        }
                        for (int i = 0; i < lines.length; i++)
                            line[i] = Float.parseFloat(lines[i]);
                        num_lines++;
                    }

                    System.out.println("Length:" + num_lines);
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n", seconds);

                System.out.println("Testing getRecords()");
                HDFS_SPLITS_LIST = dfs.findBlocksByRecords(name, 4);
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                    System.out.println(b.toString());
                    all = b.getRecords();

                    first = all[0];
                    last = all[all.length - 1];


                    for (int i = 0; i < all.length; i++) {
                        lines = all[i].split(",");
                        if (lines.length != 29) {
                            System.out.println("[ERROR] - num features:" + lines.length);
                            System.out.println(first);
                        }
                        for (int j = 0; j < lines.length; j++)
                            line[j] = Float.parseFloat(lines[j]);
                    }
                    System.out.println("Length:" + all.length);
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n", seconds);

            }
            ///----------------------------------------------------------------------------------------

            if(test_chunck) {
                int chunck  = 464;
                System.out.println("\n\n#############################\n\tTesting:  Chuncks\n#########");

                table_comparable1 = new ArrayList<String>();
                table_comparable2 = new ArrayList<String>();
                System.out.println("DefaultFS:" + defaultFS);


                System.out.println("\n\n#########\nTesting:  dfs.findBlocksByRecords\n#########");

                name = "/user/pdm2/train_0.0012m.csv"; // each line has same size
                System.out.println("Reading file:" + name + " \n");
                HDFS_SPLITS_LIST = dfs.findBlocksByRecords(name, frag);
                n_blk = HDFS_SPLITS_LIST.size();
                System.out.println("Number of HDFS BLOCKS: " + n_blk);

                System.out.println("Testing getRecord() and HasRecord()");
                HDFS_SPLITS_LIST = dfs.findBlocksByRecords(name, frag);
                byte[] bb = null;
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                    int num_lines = 0;
                    System.out.println(b.toString());
                    first = new String(b.getRecord_Chunck(chunck), "UTF-8").replace("\n","");
                    num_lines++;
                    while (b.HasRecords()) {
                        bb= b.getRecord_Chunck(chunck);
                        num_lines++;
                    }
                    last = new String(bb, "UTF-8").replace("\n","");

                    table_comparable1.add(first);
                    table_comparable1.add(last);
                    System.out.println("First:|" + first + "|\t\tLast:|" + last + "|\t\tLength:" + num_lines);
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n", seconds);

                System.out.println("\n\n#########\nTesting:  dfs.findALLBlocks\n#########");

                HDFS_SPLITS_LIST = dfs.findALLBlocks(name);
                startTime = System.nanoTime();
                for (Block b : HDFS_SPLITS_LIST) {
                    int num_lines = 0;
                    System.out.println(b.toString());
                    first = new String(b.getRecord_Chunck(chunck), "UTF-8").replace("\n","");
                    num_lines++;
                    while (b.HasRecords()) {
                        bb= b.getRecord_Chunck(chunck);
                        num_lines++;
                    }
                    last = new String(bb, "UTF-8").replace("\n","");

                    table_comparable2.add(first);
                    table_comparable2.add(last);
                    System.out.println("First:|" + first + "|\t\tLast:|" + last + "|\t\tLength:" + num_lines);
                }
                seconds = (double) (System.nanoTime() - startTime) / 1000000000.0;
                System.out.printf(" \n[INFO] Time elapsed: %.2f seconds\n----------\n\n", seconds);


                //----------------------------------------------------------------------------------------
                // INFORMATIONS
                //----------------------------------------------------------------------------------------

                int size = table_comparable1.size();
                System.out.println("Tests: " + size);
                for (int i = 0; i < size; i++) {
                    String ok = "[ERROR]";
                    if (table_comparable1.get(i).equals(table_comparable2.get(i)))
                        ok = " [OK!]";
                    System.out.println(table_comparable1.get(i) + " " + table_comparable2.get(i) + "  " + ok);
                }
            }
        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}