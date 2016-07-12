package integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.*;
import java.util.ArrayList;



public class HDFS implements Serializable {

    private FileSystem fs;
    private Configuration conf;
    private int state=0;
    private FSDataInputStream fsDataInputStream;
    private String defaultFS = "";

    public void setUserHDFS(String user){
        this.conf.set("hadoop.job.ugi", user);    // USER OF HDFS

    }


    public HDFS(String defaultFS){
            this.defaultFS = defaultFS;
            this.conf = new Configuration();
            this.conf.set("fs.defaultFS", defaultFS);
            this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
            this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
          //  this.conf.set("dfs.replication", "1");          //ONLY IF IS SINGLE NODE

            try{
                this.fs = FileSystem.get(conf);

            } catch (Exception e) {
                e.printStackTrace();
            }
    }

    public void ls (String dir){
        try {
            FileStatus[] status = fs.listStatus(new Path(dir));
            for(int i=0;i<status.length;i++){
                System.out.println(status[i].getPath());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        path = defaultFS+"/"+path;
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


    public String getSplit (int initial, int end, String path, boolean last, char delimiter){
        StringBuffer buffer = new StringBuffer();

        try{
            Path pt=new Path(path);
            FSDataInputStream fsDataInputStream = fs.open(pt);


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

            if(!last)
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


    public void setfile (String path, long pos) throws IOException {

        Path pt=new Path(path);
        fsDataInputStream = fs.open(pt);
        fsDataInputStream.seek(pos);

    }


    public String getRecord (int RecordsReadeds, long initial, int end, String path, boolean last, char delimiter) throws IOException {
        StringBuffer buffer = new StringBuffer();

        try{

            byte ch;
            boolean notfinalBlock = true;
            buffer = new StringBuffer();

            long pos = initial;

            if(RecordsReadeds == 0) { //remove the trash

                if(pos !=0) {
                    do {
                        ch = fsDataInputStream.readByte();  //read before counting position
                        pos++; //position tracker
                    }while (ch != (byte) delimiter);

                }
            }

            while (notfinalBlock){
                ch = fsDataInputStream.readByte();  //read before counting position
                pos++; //position tracker
                // System.out.println(ch);
                if(ch==((byte) delimiter)){
                    notfinalBlock = false;
                }else{
                    if(pos != (initial+end) )
                        buffer.append((char)ch);
                }

                if(last && (pos >= end))
                    notfinalBlock = false;

            }

            //System.out.println("SPLIT: "+ buffer.toString());
            state = (int) pos;
            // System.out.println("State: "+state);


        } catch (Exception e) {
            System.out.println("Error: readBlock");
            e.printStackTrace();
        }
        return buffer.toString();
    }


    public int refreshState (){
        return state;
    }

/*

    Writing Operations

 */

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

    public boolean writeFILE (String text, String dst, boolean append)  {
        try {
            fs = FileSystem.get(conf);

            Path pt=new Path(dst);
            //Destination file in HDFS
            if (!append){
                try {
                    BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
                    // TO append data to a file, use fs.append(Path f)
                    br.write(text);
                    br.close();
                    System.out.println(dst + " writed");
                    return true;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }else{
                BufferedWriter br;
                DistributedFileSystem myDfs = (DistributedFileSystem)fs;
                do {

                    myDfs.isFileClosed(pt);

                }while(!myDfs.isFileClosed(pt));
                System.out.println("Not Busy");
                br = new BufferedWriter(new OutputStreamWriter(fs.append(pt)));


                br.write(text);
                br.close();
                System.out.println(dst + " appended");
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


}
