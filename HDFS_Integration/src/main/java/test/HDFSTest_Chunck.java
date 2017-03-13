package test;

import integration.Block;
import integration.HDFS;

import java.util.ArrayList;

/**
 * Created by lucasmsp on 16/05/16.
 */


public class HDFSTest_Chunck {



    public static void main(String args[]) {
         String defaultFS = "hdfs://localhost:9000";//System.getenv("MASTER_HADOOP_URL"); // local of the HDFS's master node

         try {

             System.out.println("DefaultFS:" + defaultFS);
             HDFS dfs =  new HDFS(defaultFS);

             System.out.println(" ");
             String fileHDFS = "/user/pdm2/train_0.001m_cleaned.csv ";  //Means that the file is in the root of hdfs

             ArrayList<Block> HDFS_SPLITS_LIST = dfs.findBlocksByRecords(fileHDFS,4);
             int n_blk = HDFS_SPLITS_LIST.size();
             System.out.println("Number of HDFS BLOCKS: " + n_blk);

             String record[];
             /*
                    FIRST BLOCK
            */

             int n_bytes = 464;
             record = HDFS_SPLITS_LIST.get(0).getRecords('\n');


       //      System.out.println("First line of first block:" + new String(record, "UTF-8"));
/*
             while(HDFS_SPLITS_LIST.get(0).HasRecords())
                 record = HDFS_SPLITS_LIST.get(0).getRecord_Chunck(9*10000);
             System.out.println("Last line of first block:" +new String(record, "UTF-8"));



             record = HDFS_SPLITS_LIST.get(1).getRecord_Chunck(9*10000);
             System.out.println("First line of second block:" + new String(record, "UTF-8"));


             while(HDFS_SPLITS_LIST.get(1).HasRecords())
                 record = HDFS_SPLITS_LIST.get(1).getRecord_Chunck(9*10000);
             System.out.println("Last line of second block:" + new String(record, "UTF-8"));




             record = HDFS_SPLITS_LIST.get(2).getRecord_Chunck(9*10000);
             System.out.println("First line of third block:" + new String(record, "UTF-8"));

             while(HDFS_SPLITS_LIST.get(2).HasRecords())
                 record = HDFS_SPLITS_LIST.get(2).getRecord_Chunck(9*10000);
             System.out.println("Last line of third block:" + new String(record, "UTF-8"));


             */
             System.out.println("\n\nFIM");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}