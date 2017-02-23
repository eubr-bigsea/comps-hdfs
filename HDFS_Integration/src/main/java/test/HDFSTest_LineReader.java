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

         try {

             System.out.println("DefaultFS:" + defaultFS);
             HDFS dfs =  new HDFS(defaultFS);

             System.out.println(" ");

             String trainingSet_name = "/user/pdm2/higgs-train-0.1m.csv";

             ArrayList<Block> HDFS_SPLITS_LIST = dfs.findALLBlocks(trainingSet_name);
             int n_blk = HDFS_SPLITS_LIST.size();
             System.out.println("Number of HDFS BLOCKS: " + n_blk);


             for (Block b : HDFS_SPLITS_LIST)
                 System.out.println(b.toString());
             System.out.println(" ");

             int i=0;


             for (Block b : HDFS_SPLITS_LIST)
                 if (b.HasRecords()) {
                     String w = b.getRecord();
                     String[] words = w.split(",");

                     if (words.length != 29)
                         System.out.println("i:"+i+" | " +w);

                     double f = Math.round(Float.parseFloat(words[0]) );
                     double[]  bb = new double[words.length - 1];
                     for(int j = 1; j < words.length; j++)
                         bb[j-1] = Double.parseDouble(words[j]);
                     i++;
                 }

             System.out.println("Primeiro arquivo lido!\n");
             HDFS_SPLITS_LIST = dfs.findALLBlocks("/user/pdm2/higgs-test-0.1m.csv");
             for (Block b : HDFS_SPLITS_LIST)
                 if (b.HasRecords()) {
                     String w = b.getRecord();
                     String[] words = w.split(",");
                     if (words.length != 29)
                         System.out.println("i:"+i+" | " +w);
                     i++;

                     double f = Math.round(Float.parseFloat(words[0]) );
                     double[]  bb = new double[words.length - 1];
                     for(int j = 1; j < words.length; j++)
                         bb[j-1] = Double.parseDouble(words[j]);
                     i++;
                 }



             System.out.println("\n\nFIM");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}