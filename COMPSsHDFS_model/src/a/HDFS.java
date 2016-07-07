package a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;


import java.util.ArrayList;
import java.io.*;



/**
 * Created by lucasmsp on 17/05/16.
 */

public class HDFS implements java.io.Serializable {

    private FileSystem fs;
    private Configuration conf;

    public HDFS(String defaultFS){

            this.conf = new Configuration();
            this.conf.set("fs.defaultFS", defaultFS);
            this.conf.set("hadoop.job.ugi", "lucasmsp");    // USER OF HDFS
            this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
            this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
            this.conf.set("dfs.replication", "1");          //ONLY IF IS SINGLE NODE

            try{
                this.fs = FileSystem.get(conf);

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

    public void  createFile(String dst, boolean overwrite) throws IOException {
        fs = FileSystem.get(conf);
        Path pt=new Path(dst);
        fs.create(pt, overwrite);
        fs.close();

    }

    /*
        in:     Path of the file
        out:    String with all the contents of the file
    */
    public String readALLFiletoString (String path){
        String all ="";
        BufferedReader br =null;
        try{
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



    /*
        in:     Path of the file
        out:    List of all blocks of the file
     */
    public ArrayList<Bloco> findALLBlocks (String path){
        ArrayList<Bloco> b = new ArrayList<Bloco>();
        try{

            FileStatus Status;
            BlockLocation[] bLocations;
            Path file = new Path(path);
            FileStatus fStatus = fs.getFileStatus(file);
            Status=fStatus;

            bLocations= fs.getFileBlockLocations(Status, 0, Status.getLen());
            int number_block = 0;

            for(BlockLocation aLocation : bLocations){
                Bloco c = new Bloco();
                c.setStart(aLocation.getOffset());
                c.setEnd(aLocation.getLength());
                c.setPath(path);
                c.setIndex(number_block);

                if ((number_block +1) == bLocations.length )               c.setTheLast(true);
                else                                                       c.setTheLast(false);

                b.add(c);
                number_block++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return b;
    }

    /*
           in:     Block and how the file is splitted
           out:    String with the data stored in this block
    */

    public String readBlock (Bloco x, char delimiter){
        StringBuffer buffer = new StringBuffer();

        try{
            Path pt=new Path(x.getPath());
            FSDataInputStream fsDataInputStream = fs.open(pt);

                int initial = (int) x.getStart();
                int end = (int) x.getEnd();
                System.out.println("HDFS.readBlock = ["+initial + " "+ end+"]" );

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
                        fsDataInputStream.seek(pos);        //set position
                        ch = fsDataInputStream.readByte();  //read before counting position
                        pos++; //position tracker
                       // System.out.println(ch);
                        if(ch==((byte) delimiter)){
                            notfinalBlock = false;
                        }else{
                            if(pos != (initial+end) )
                                buffer.append((char)ch);
                        }

                    }

                if(initial != 0) { //remove the trash
                    int posi = buffer.indexOf(delimiter+""); //find first delimiter
                    buffer.delete(0,posi);                  //delete util this part
                    //System.out.println("\\n at " + posi);
                }




        } catch (Exception e) {
            System.out.println("Error: readBlock");
            e.printStackTrace();
        }
        return buffer.toString();
    }


    public ArrayList<Integer> readBlockToInt(Bloco x, char delimiter) throws IOException {

        ArrayList<Integer> texts = new ArrayList<Integer>();

        try{
            Path pt=new Path(x.getPath());
            FSDataInputStream fsDataInputStream = fs.open(pt);

            int initial = (int) x.getStart();
            int end = (int) x.getEnd();
            System.out.println("HDFS.readBlock = ["+initial + " "+ end+"]" );


            fsDataInputStream.seek(initial);
            char ch = '#';
            System.out.println("value  initial:  " + initial);
            if(initial != 0) { // Remove the trash before the logical block
                System.out.println("on");
                do {
                    ch = fsDataInputStream.readChar();

                }while(!(ch == delimiter));

                System.out.println("CHar "+ ch);
                System.out.println("");
            }

            int pos;
            int text;
            System.out.println("NOw:" + fsDataInputStream.getPos());
            do{
                text = fsDataInputStream.readInt();
                pos = (int) fsDataInputStream.getPos();

                texts.add(text);
            }while(pos <= end);

            System.out.println(text);
        } catch (Exception e) {
            System.out.println("Error: readBlocktoInt");
            e.printStackTrace();
        }
        return texts;
    }


}
