package a;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;



public class Sample {


	public static void main ( String[] args ) throws Exception {

		// ------------------------------------------------------------//
		// HDFS Integration     										//
		// ------------------------------------------------------------//

		String defaultFS = "hdfs://localhost:9000";
		HDFS dfs =  new HDFS(defaultFS);


		String fileHDFS = "hdfs://localhost:9000/file_teste3.in";
		ArrayList<Bloco> HDFS_BLOCKS = dfs.findALLBlocks(fileHDFS);
		int n_blk = HDFS_BLOCKS.size();
		System.out.println("Number of HDFS BLOCKS: " + n_blk);


		// ----------------------------------------------//
		// Execution of the program 					 //
		// ----------------------------------------------//


		for(Bloco b : HDFS_BLOCKS) {

			String file1 = "output"+b.getIndex()+".txt";
			String file2 = "output"+b.getIndex()+"_sum.txt";

			SampleImpl.conquister(b, file1);
			SampleImpl.sum(file1,file2);

		}



	}
}
