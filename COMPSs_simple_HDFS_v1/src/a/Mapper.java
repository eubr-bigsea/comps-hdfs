package a;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.HashMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;


/**
 * Created by lucasmsp on 16/06/16.
 */

public class Mapper {

    private boolean cluster;
    private ArrayList<String> node;
    private String defaultFS;
    private Handler_HDFS hdfs;
    private int number_bloco;

    public Mapper(boolean cluster, String defaultFS) {
        this.hdfs = new Handler_HDFS(defaultFS);
        this.defaultFS = defaultFS;
        this.cluster= cluster;
        this.number_bloco = 0;
        node = new ArrayList<String>();

        if(cluster){
            parser();
        }else{
            node.add("localhost");
        }


    }


    public void parser () {

        try {
            DocumentBuilderFactory factory  =   DocumentBuilderFactory.newInstance();
            DocumentBuilder builder         = factory.newDocumentBuilder();
            Document document =  builder.parse("/home/lucasmsp/workspace/COMPSs_simple_HDFS_v1/out/artifacts/COMPSs_simple_HDFS_v1_jar/project.xml");
            NodeList nodeList = document.getDocumentElement().getChildNodes();

            for (int i = 0; i < nodeList.getLength(); i++) {
                Node elemento = nodeList.item(i);
                if (elemento instanceof Element) {
                    //System.out.println(elemento.getAttributes().getNamedItem("Name").getNodeValue());
                    node.add(elemento.getAttributes().getNamedItem("Name").getNodeValue());
                }
            }
        }catch (Exception e){
            e.printStackTrace();

        }

    }


    /*
        Para cada nó que esteja no cluster COMPSs, irá procurar os blocos que ele possui sobre o arquivo
     */
    public HashMap<String, ArrayList<Bloco>> getlist (String fileHDFS){
        HashMap<String, ArrayList<Bloco>> table = new HashMap<String, ArrayList<Bloco>>();

        for (int i =0; i<node.size();i++){
            System.out.println(node.get(i));
            ArrayList<Bloco> b = hdfs.findBlocks(fileHDFS,node.get(i));
          //  System.out.println(b.toString());
            table.put(node.get(i),b);
        }


        // Falta implementar a atribuição das tarefas


        number_bloco = hdfs.getNumber_block();
        return table;
    }

    public int get_numberblocks(){

        return number_bloco;
    }


}
