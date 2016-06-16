package a;


import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;


//import SimpleItf;



public class SimpleImpl {

	public static void increment ( String counterFile, String defaultFS, String fileHDFS, HashMap<String, ArrayList<Bloco>> mapa, int item) throws FileNotFoundException, IOException {

			InetAddress ip;
			String node;

			ip = InetAddress.getLocalHost();
			node = ip.getHostName();
			//System.out.println("Your current IP address : " + ip);
		    System.out.println("Your current Hostname : " + node);


			FileInputStream fis = new FileInputStream (counterFile);
			int count = fis.read();
			fis.close();

			System.out.println("Mapeamento:" + mapa.toString());
			System.out.println("Trabalhando com o bloco "+ item);
			// Instanciando o hdfs
			Handler_HDFS HDFS = new Handler_HDFS(defaultFS);


			Bloco x = mapa.get(node).get(item);

			String content = HDFS.readFileMyDatainBlocks(fileHDFS,x,'\n');
			OutputStream fos = new FileOutputStream (counterFile);


			fos.write (content.getBytes(Charset.forName("UTF-8")));
			fos.close();


	}
}
