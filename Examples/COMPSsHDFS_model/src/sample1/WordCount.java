package sample1;

import integration.Bloco;
import integration.HDFS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class WordCount {


	static String defaultFS = "hdfs://localhost:9000"; //local of the HDFS's master node

	public static void main ( String[] args ) throws Exception {

		// ------------------------------------------------------------//
		// HDFS Integration     										//
		// ------------------------------------------------------------//

		HDFS dfs =  new HDFS(defaultFS);
		String fileHDFS = "text_huge.in";  //Means that the file is in the root of hdfs

		ArrayList<Bloco> HDFS_SPLITS_LIST = dfs.findALLBlocks(fileHDFS);
		int n_blk = HDFS_SPLITS_LIST.size();
		System.out.println("Number of HDFS BLOCKS: " + n_blk);

		// ----------------------------------------------//
		// Execution of the program 					 //
		// ----------------------------------------------//

		HashMap<String, Integer> result = new HashMap<String, Integer>();
		System.out.println("[LOG] Computing result");
		for(Bloco b : HDFS_SPLITS_LIST) {
			HashMap<String, Integer> partialResult = map(b);
			result = mergeResults(partialResult, result);
		}
		System.out.println("[LOG] Result size = " + result.keySet().size());

		write(result,"output_wc.txt");

	}

	public static void write(HashMap<String, Integer> m1, String path) {
		String msg = "";
		for (String key : m1.keySet()) {
			 msg = msg + "("+key + ", " + m1.get(key)+ ")\n";
		}
		HDFS dfs = new HDFS(defaultFS);
		dfs.writeFILE(msg, path,false);

	}

	public static HashMap<String, Integer> mergeResults(HashMap<String, Integer> m1, HashMap<String, Integer> m2) {
		Iterator it1 = m1.entrySet().iterator();
		while (it1.hasNext()) {
			Map.Entry pair = (Map.Entry)it1.next();
			if(m2.containsKey(pair.getKey())){
				m2.put( pair.getKey().toString(), (m1.get(pair.getKey())) + m2.get(pair.getKey())  );
			}
			else{
				m2.put( pair.getKey().toString(), m1.get(pair.getKey()) );
			}
			it1.remove(); // avoids a ConcurrentModificationException
		}

		return m2;
	}



	public static HashMap<String, Integer> map(Bloco blk) {

		HashMap<String, Integer> res = new HashMap<String, Integer>();
		while (blk.HasRecords()){
			String[] words = blk.getRecord().split(" ");
			for (String word : words) {
				if (res.containsKey(word)) {
					res.put(word, res.get(word) + 1);
				} else {
					res.put(word, 1);
				}
			}
		}

		return res;
	}
}
