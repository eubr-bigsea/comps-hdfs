package a;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;


public class SampleImpl {

	public static void conquister (Bloco blk, String output) throws IOException {


			System.out.println("Block: "+ blk.toString());

			String defaultFS = "hdfs://localhost:9000"; //path to the default FS
		    HDFS dfs =  new HDFS(defaultFS);
			String content = dfs.readBlock(blk,'\n');

			String[] S_numbers = content.split("\n");


			int tmp;
			int number=0;
			for (String n_temp : S_numbers) {
				if(!n_temp.isEmpty()) {
					tmp = Integer.parseInt(n_temp);
					if (number < tmp)
						number = tmp;
				}
			}


			System.out.println("Partial Result of block" +blk.getIndex()+": " + number);
			FileOutputStream fos = new FileOutputStream(output);
			fos.write((""+number).getBytes());
			fos.close();

	}


	public static void sum (String file, String output) throws IOException {
		int n_temp =0;


		Scanner scanner = new Scanner(new File(file));
		while(scanner.hasNextInt())
			n_temp = scanner.nextInt();
		scanner.close();

		FileOutputStream fos = new FileOutputStream(output);
		fos.write(("" + (n_temp+10)).getBytes());
		fos.close();

	}


}
