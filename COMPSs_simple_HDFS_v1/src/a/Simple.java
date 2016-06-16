package a;
import java.io.BufferedReader;
import java.io.FileInputStream ;
import java.io.FileOutputStream ;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;


public class Simple {


	public static void main ( String[] args ) throws Exception {

		// ------------------------------------------------------------//
		// HDFS Integration     										//
		// ------------------------------------------------------------//

		String defaultFS = "hdfs://localhost:9000";
		boolean cluster = true;
		String fileHDFS = "hdfs://localhost:9000/file_teste3.in";
		Mapper map = new Mapper(cluster,defaultFS);
		HashMap<String, ArrayList<Bloco>> mapeamento = map.getlist(fileHDFS);
		int blocos = map.get_numberblocks();
		System.out.println("Quantidade de blocos para o arquivo:" + blocos);


		String counterName = "file_created";
		String msg = args[0]+"\n";

		// ------------------------------------------------------------//
		// Creation of the file which will contain the counter variable //
		// ------------------------------------------------------------//
		
			FileOutputStream fos = new FileOutputStream ( counterName );
			fos.write( msg.getBytes(Charset.forName("UTF-8") ));
			System.out.println ("Arquivo criado um valor inicial de: "+ msg );
			fos.close ();



			//System.out.println(content);
		// ----------------------------------------------//
		// Execution of the program //
		// ----------------------------------------------//
		for(int i=0; i<blocos;i++) {
			SimpleImpl.increment(counterName, defaultFS, fileHDFS, mapeamento,i);
		}
		// ----------------------------------------------//
		// Reading from an object stored in a File //
		// ----------------------------------------------//
		
		FileInputStream fis = new FileInputStream ( counterName );
		System.out.println (" Teste v2 "+ fis.read ());
		fis.close ();

	}
}
