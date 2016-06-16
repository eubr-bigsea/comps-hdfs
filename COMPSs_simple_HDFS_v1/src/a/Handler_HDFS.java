package a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * Created by lucasmsp on 17/05/16.
 */

public class Handler_HDFS {

    public FileSystem fs;
    public int number_block;
    public  Handler_HDFS(String defaultFS) {
        try{
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", defaultFS);
            conf.set("hadoop.job.ugi", "lucasmsp");
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
            fs = FileSystem.get(conf);
            number_block = 0;

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    public void ls (String dir){
        try {
            //Listar o conteudo de um diretorio
            FileStatus[] status = fs.listStatus(new Path(dir));
            for(int i=0;i<status.length;i++){
                System.out.println(status[i].getPath());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void mkdir (String path){
        try{
            fs.createNewFile(new Path(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public String readFileString (String path){
        String all ="";
        BufferedReader br =null;
        try{
            //Ler um arquivo inteiro
            Path pt=new Path(path);
            FSDataInputStream fsDataInputStream = fs.open(pt);
            br = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String line;
            line=br.readLine();

            while (line != null){
                //System.out.println(line);
                all +=line+"\n";
                line=br.readLine();

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return all;
    }




    public ArrayList<Bloco> findBlocks (String path, String node){
        ArrayList<Bloco> b = new ArrayList<Bloco>();
        try{

            //Descobrir os blocos de um file
            FileStatus Status;
            BlockLocation[] bLocations;
            StringBuilder sb = new StringBuilder();
            Path file = new Path(path);
            FileStatus fStatus = fs.getFileStatus(file);
            Status=fStatus;

            bLocations= fs.getFileBlockLocations(Status, 0, Status.getLen());
            number_block = 0;
            for(BlockLocation aLocation : bLocations){
                if(aLocation.toString().contains(node)) {
                    if ((number_block +1) == bLocations.length ){
                        b.add(new Bloco(aLocation.getOffset(),  aLocation.getLength(),true));
                    }else
                        b.add(new Bloco(aLocation.getOffset(),  aLocation.getLength(),false));
                }
                number_block++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return b;
    }

    public int getNumber_block(){
        return number_block;
    }

    public String readFileMyDatainBlocks (String path, Bloco x, char delimitador){
        StringBuffer buffer = new StringBuffer();
        try{
            Path pt=new Path(path);
            FSDataInputStream fsDataInputStream = fs.open(pt);

                int initial = (int) x.getStart();
                int end = (int) x.getEnd();
                System.out.println("Handler_HDFS.readFileBlock = ["+initial + " "+ end+"]" );

                byte ch;
                boolean notfinalBlock = true;
                buffer = new StringBuffer();

                byte[] buff = new byte[end];
                System.out.println(buff.length);
                fsDataInputStream.read(initial, buff, 0, end);
                buffer.append(new String(buff, "UTF-8")); //Copy all block
                System.out.println("Length current :" + buffer.length());


                long pos =  initial+end;

                if(!x.istheLast())
                    while (notfinalBlock){
                        fsDataInputStream.seek(pos);  //set position
                        ch = fsDataInputStream.readByte();//read before counting position
                        pos++; //position tracker
                        //System.out.println(ch);
                        if(ch==((byte) delimitador)){
                            notfinalBlock = false;
                        }else{
                            if(pos != (initial+end) )
                                buffer.append((char)ch);
                        }

                    }

                if(initial != 0) { //remove the trash
                    int posi = buffer.indexOf("\n"); //find first \n
                    buffer.delete(0,posi);
                    // System.out.println("\\n at " + posi);
                }




        } catch (Exception e) {
            e.printStackTrace();
        }
        return buffer.toString();
    }

}
