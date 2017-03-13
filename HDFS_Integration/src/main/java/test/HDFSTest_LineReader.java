package test;

import integration.Block;
import integration.HDFS;


import java.util.ArrayList;
import java.lang.*;
import java.util.StringTokenizer;

/**
 * Created by lucasmsp on 16/05/16.
 */


public class HDFSTest_LineReader {

    static String defaultFS = System.getenv("MASTER_HADOOP_URL"); // local of the HDFS's master node

    public static void main(String args[]) {
            String error="";
        int indice = 0;
        String s = "";
        StringTokenizer tokenizer;
         try {

             System.out.println("DefaultFS:" + defaultFS);
             HDFS dfs =  new HDFS(defaultFS);

             System.out.println(" ");

//             String trainingSet_name = "/user/pdm2/train_0.001m_cleaned.csv";
//
//             ArrayList<Block> HDFS_SPLITS_LIST = dfs.findALLBlocks(trainingSet_name);
//             int n_blk = HDFS_SPLITS_LIST.size();
//             System.out.println("Number of HDFS BLOCKS: " + n_blk);
//
//
//             for (Block b : HDFS_SPLITS_LIST)
//                 System.out.println(b.toString());
//             System.out.println(" ");
//
//             int i=0;
//
//
//             for (Block b : HDFS_SPLITS_LIST)
//                 if (b.HasRecords()) {
//                     String w = b.getRecord();
//                     String[] words = w.split(",");
//
//                     if (words.length != 29)
//                         System.out.println("i:"+i+" | " +w);
//
//                     double f = Math.round(Float.parseFloat(words[0]) );
//                     double[]  bb = new double[words.length - 1];
//                     for(int j = 1; j < words.length; j++)
//                         bb[j-1] = Double.parseDouble(words[j]);
//                     i++;
//                 }

             System.out.println("Primeiro arquivo lido!\n");
             ArrayList<Block> HDFS_SPLITS_LIST = dfs.findBlocksByRecords("/user/pdm2/train_0.1m_cleaned.csv",16);
//             for (Block b : HDFS_SPLITS_LIST) {
//                 String[] all = b.getRecords('\n');
//                 System.out.println("\n\n"+b.toString());
//                 System.out.println("--------\nsize:"+ all.length);
//                 System.out.println("All 1:" +all[0].length()+" | "+ all[0]+"\n\n");
//                 System.out.println("All end:" +all[all.length-1].length()+"\n\n"+ all[all.length-2]+"\n"+all[all.length-1]);
//             }
             int lab =0;
             double f =0;

             for (Block blk : HDFS_SPLITS_LIST) {
                 System.out.println(blk.toString());
                 String[] lines = blk.getRecords('\n');

                 System.out.println("Number lines: " + lines.length);

                 for (int l = 0; l<lines.length; l++) {
                     error = lines[l];
                     indice = l;

                    // System.out.println("L" + l + ": " + );
                     tokenizer = new StringTokenizer(lines[l], ",");
                     if (tokenizer.countTokens() != 29){
                        System.out.println(tokenizer.countTokens()+"  >>"+error);
                     }
                     s = tokenizer.nextToken();
                     if(s.equals("+0.000000000000"))
                        lab = 0;
                     else
                        lab =1;
                     for (int i = 0; tokenizer.hasMoreTokens(); i++)
                         f = Double.parseDouble(tokenizer.nextToken());
                 }

             }
             System.out.println("\n\nFIM");

        } catch (Exception e) {
             System.out.println("error ["+indice+"]:"+error+"\n\n");
            e.printStackTrace();
        }
    }
}