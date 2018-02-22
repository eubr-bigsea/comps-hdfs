package test;

import integration.Block;
import integration.HDFS;
import storage.StorageItf;


import java.util.ArrayList;
import java.lang.*;
import java.util.List;


/**
 * Created by lucasmsp on 16/05/16.
 */


public class HDFSTest_LineReader {

    static String defaultFS = System.getenv("MASTER_HADOOP_URL"); // local of the HDFS's master node

    public static void main(String args[]) {

        boolean test1 = false;
        boolean test2 = false;
        boolean test3 = false;

        for (int i=0; i<args.length;i++){
            if (args[i].equals("1"))
                test1 = true;
            if (args[i].equals("2"))
                test2 = true;
            if (args[i].equals("3"))
                test3 = true;
        }
        if (test1)
            test01_mkdir_and_write();
        if (test2)
            test02_merge();
        if (test3)
            test03_read();
    }


    public static void test01_mkdir_and_write(){
        int numFrag = 4;
        System.out.println("Test01 - write");
        HDFS dfs =  new HDFS(defaultFS);
        dfs.mkdir("/JavaIntegration/");

        String dst = "/JavaIntegration/javaFile";
        String msg =
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
                "Donec euismod ex sapien, eget dignissim dolor rhoncus eu.\n" +
                "Pellentesque bibendum suscipit ante sit amet finibus. " +
                "Donec tincidunt lectus maximus feugiat vestibulum.\n";



        for (int i=0; i< numFrag; i++){
            String dstFile = dst +"_" + i;
            System.out.println("Writing file: "+dstFile);
            dfs.writeFILE(msg, dstFile, false);
        }

        System.out.println( dfs.ls("/JavaIntegration/"));
    }

    public static void test02_merge(){
        System.out.println("Test02 - merge");
        HDFS dfs =  new HDFS(defaultFS);
        String src = "/JavaIntegration/";
        String dst = "/JavaIntegration.txt";
        dfs.mergeFiles(src,dst,true);
    }

    public static void test03_read(){
        System.out.println("Test02 - read");
        HDFS dfs =  new HDFS(defaultFS);
        String path = "/JavaIntegration.txt";
        ArrayList<Block> HDFS_SPLITS_LIST = dfs.findBlocksByRecords(path, 4);
        for (Block block: HDFS_SPLITS_LIST) {
            System.out.println(block.toString());
            String[] text = block.getRecords();
            for (int i = 0; i< text.length;i++)
                System.out.println(text[i]);
        }
    }


}