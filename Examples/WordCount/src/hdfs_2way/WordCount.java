package hdfs_2way;

import integration.Block;
import integration.HDFS;
import storage.StorageItf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;




public class WordCount {


	public static StorageItf dfs;

	public static void main ( String[] args ) throws Exception {

		Boolean list = false;
		// Get and parse arguments
		int argIndex = 0;
		while (argIndex < args.length) {
			String arg = args[argIndex++];
			if (arg.equals("-list")) {
				list = true;
			}
		}


		// ------------------------------------------------------------//
		// HDFS Integration     										//
		// ------------------------------------------------------------//

		ArrayList<Block> HDFS_SPLITS_LIST = dfs.getBlocks(0);
		int n_blk = HDFS_SPLITS_LIST.size();
		System.out.println("Number of HDFS BLOCKS: " + n_blk);

		// ----------------------------------------------//
		// Execution of the program 					 //
		// ----------------------------------------------//

		HashMap<String, Integer> result = new HashMap<String, Integer>();
		System.out.println("[LOG] Computing result");
		for(Block b : HDFS_SPLITS_LIST) {
			HashMap<String, Integer> partialResult = map(b);
			result = mergeResults(partialResult, result);
		}
		System.out.println("[LOG] Result size = " + result.keySet().size());

		System.out.println("[LOG] Result size = " + result.keySet().size());
		if (list){
			for (String key : result.keySet()){
				System.out.println("'"+key+"', "+result.get(key));
			}
		}

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



	public static HashMap<String, Integer> map(Block blk) {

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
