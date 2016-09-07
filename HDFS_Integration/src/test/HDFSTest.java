package test;

import integration.Bloco;
import integration.HDFS;

import java.util.ArrayList;
import java.lang.*;
/**
 * Created by lucasmsp on 16/05/16.
 */


public class HDFSTest {

    static String defaultFS = System.getenv("MASTER_HADOOP_URL"); // local of the HDFS's master node

    public static void main(String args[]) {

         try {

             System.out.println("DefaultFS:" + defaultFS);
             HDFS dfs =  new HDFS(defaultFS);


             String fileHDFS = "file_200mb.in";  //Means that the file is in the root of hdfs

             ArrayList<Bloco> HDFS_SPLITS_LIST = dfs.findALLBlocks(fileHDFS);
             int n_blk = HDFS_SPLITS_LIST.size();
             System.out.println("Number of HDFS BLOCKS: " + n_blk);

             String[] words = HDFS_SPLITS_LIST.get(0).getRecord().split(" ");

             System.out.println(words[0]);





             System.out.println("FIM");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}