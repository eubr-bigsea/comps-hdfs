package integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.*;


import org.apache.hadoop.util.LineReader;

import java.io.*;
import java.util.ArrayList;

import java.io.Serializable;

public class HDFS implements Serializable {

    //private static final long serialVersionUID = 1L;

    private Configuration conf;
    private FileSystem fs;
    private FSDataInputStream fsDataInputStream;
    private Path path = null;
    private String defaultFS = "";
    private LineReader reader = null;
    private boolean ended = false;
    private long state=0;
    public long bytes_readeds = 0;
    private boolean debug =false;
    public long fragment_size = 0;


    public HDFS(String defaultFS){
        this.defaultFS = defaultFS;
        //System.out.println("DefaultFS recebido:" + defaultFS);
        this.conf = new Configuration();
        this.conf.set("fs.defaultFS", defaultFS);
        this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
        this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
        this.fragment_size = 67108864;
        // The size of the block is not always the same of the cluster size
        //System.out.println(conf.get("dfs.blocksize"));


        //  this.conf.set("dfs.replication", "1");          //ONLY IF IS SINGLE NODE

        try{
            this.fs = FileSystem.newInstance(conf);

            //.get(conf);

        } catch (Exception e) {
            System.out.println("Error: HDFS's constructor");
            e.printStackTrace();
        }
    }

    /*---------------------------------------------------------------------------------------

                                    Management Operations

     ---------------------------------------------------------------------------------------*/

    /* - setUserHDFS
            @Method: Set the user of the hdfs
            @in: user's name
    */
    public void setUserHDFS(String user){
        this.conf.set("hadoop.job.ugi", user);

    }

    /* - ls
            @Method: Return a list of all files in the folder
            @in:     Folder's path
            @out:    String with all the contents of the file
     */
    public ArrayList<String> ls (String dir){
        ArrayList<String> files = new ArrayList<String>();
        try {
            FileStatus[] status = fs.listStatus(new Path(dir));
            for(int i=0;i<status.length;i++){
                files.add(status[i].getPath().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return files;
    }

    /* - mkdir
            @Method: create a new folder on the HDFS
            @in: The path
            @Alert: You need to create the folders in order
     */
    public void mkdir (String path){
        try{
            fs.createNewFile(new Path(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /* - findALLBlocks
        @Method: create a list of Blocks of a file in HDFS
        @in:     Path of the file
        @out:    List of all blocks of the file
    */
    public ArrayList<Block> findALLBlocks (String path){
        path = defaultFS+path;
        if (debug)
            System.out.println("Current Path: " + path);
        ArrayList<Block> b = new ArrayList<Block>();
        try{

            FileStatus Status;
            BlockLocation[] bLocations;
            Path file = new Path(path);
            FileStatus fStatus = fs.getFileStatus(file);
            Status=fStatus;

            bLocations= fs.getFileBlockLocations(Status, 0, Status.getLen());
            int number_block = 0;

            for(BlockLocation aLocation : bLocations){

                Block c = new Block();
                c.setStart(aLocation.getOffset());
                c.setEnd(aLocation.getLength());
                c.setDefaultFS(defaultFS);

                if ((number_block +1) == bLocations.length )               c.setTheLast(true);
                else                                                       c.setTheLast(false);

                c.setPath(path);
                c.setIndex(number_block);

                number_block++;

                b.add(c);

            }

        } catch (Exception e) {
            System.out.println("[ERROR] - HDFS.findALLBlocks()");
            e.printStackTrace();
        }

        return b;
    }


    /* - findBlocksByRecords
    @Method: create a list of Blocks of a file in HDFS
    @in:     Path of the file
    @out:    List of blocks splitted by number of nodes
    */
    public ArrayList<Block> findBlocksByRecords (String path, int nodes){
        path = defaultFS+path;
        if (debug)
            System.out.println("Current Path: " + path + " Nodes:"+nodes);
        ArrayList<Block> blocks_list = new ArrayList<Block>();
        try{
            FileStatus Status = fs.getFileStatus(new Path(path));

            long size = Status.getLen();
            long blockSize = size / nodes;
            int number_block = 0;
            long start  = 0;
            long offset = 0;

            if (debug) System.out.println("size: " + size + " BlckSize:"+blockSize);

            while(number_block < nodes){

                start += offset;
                size -= blockSize;
                offset = ((number_block +1) == nodes) ? blockSize+size : blockSize;
                if (debug) System.out.println("Start:" + start + " Offset:"+ offset);

                Block c = new Block();
                c.setStart(start);
                c.setEnd(offset);
                c.setDefaultFS(defaultFS);
                if ((number_block +1) == nodes )      c.setTheLast(true);
                else                                  c.setTheLast(false);
                c.setPath(path);
                c.setIndex(number_block);

                blocks_list.add(c);
                number_block++;
            }

        } catch (Exception e) {
            System.out.println("[ERROR] - HDFS.findBlocksByRecords()");
            e.printStackTrace();
        }

        return blocks_list;
    }

    public void closeConection()  {
       // try {
            //fsDataInputStream.close();
           // fs.close();
            System.out.println("Try to close conections");
     //   } catch (IOException e) {
     //       System.out.println("[ERROR] - HDFS.closeConection()");
     //       e.printStackTrace();
    //    }

    }

    /*---------------------------------------------------------------------------------------

                                    Read Operations

     ---------------------------------------------------------------------------------------*/


    /* - readALLFiletoString
            @Method: Return all the file in a String
            @in:     Path of the file
            @out:    String with all the contents of the file
    */
    public String readALLFiletoString (String path){
        String text ="";
        BufferedReader br =null;
        Path pt=new Path(path);

        /*
            * Conferir de qual forma fica mais rapido
            *
                ContentSummary cSummary;
                try (FileSystem hdfs = pt.getFileSystem(conf)) {
                    cSummary = hdfs.getContentSummary(pt);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                long length = cSummary.getLength();
                         fsDataInputStream.readFully(0,    );
        */
        try{
            FSDataInputStream fsDataInputStream = fs.open(pt);
            br = new BufferedReader(new InputStreamReader(fsDataInputStream));

            String line;
            line=br.readLine();
            while (line != null){
                text +=line+"\n";
                line=br.readLine();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return text;
    }



    public String  readLineText( long start, long offset ) throws Exception {
        String split = null;
        Text buffer =  new Text();

        try {
            if(bytes_readeds ==0) {                                        //Initial Split
                if(start !=  0)   bytes_readeds+=reader.readLine(buffer);  //remove the trash of the previous block;
            }

            bytes_readeds+= reader.readLine(buffer);
            split = buffer.toString();

            if (bytes_readeds >= offset )
                this.ended = true;

        } catch (EOFException e) {

        }
        return split;

    }





    /* - getSplit
            @Method: Return a split (logical block)
            @in:     The initial and final block's position, the path, the logical's delimiter,
                     and the info if this is a last block
            @out:    The text stretch respective to that block
    */
    public String getSplit (long initial, long length, String path, boolean last, char delimiter){

        try{
            Path pt=new Path(path);
            FSDataInputStream fsDataInputStream = fs.open(pt);
            long pos =  0;
            if (debug)
                System.out.println("HDFS.getSplit = ["+initial + " "+ length+"]" );

            StringBuffer buffer = new StringBuffer();
            if(initial ==0) {
                //Copy all block
                byte[] buff = new byte[(int) length];
                fsDataInputStream.read(initial, buff, 0, (int) length);
                buffer.append(new String(buff, "UTF-8"));
                pos =  initial+length;
            }else{
                //Copy all block
                byte[] buff = new byte[(int) length+2];
                fsDataInputStream.read(initial-1, buff, 0, (int) length);
                buffer.append(new String(buff, "UTF-8"));
                pos =  initial+length-1;
            }

            //Find the logical end of this text stretch
            boolean notfinalBlock = true;
            if(!last) {
              //  System.out.println("ultimo char:"+buffer.substring(buffer.length()-4,buffer.length()-1) +"=");
                if (!buffer.substring(buffer.length()-1).equals(delimiter+"")) { //verify if is ended
                    byte[] buff = new byte[1024];
                    while (notfinalBlock) {
                        fsDataInputStream.read(pos, buff, 0, 1024);
                        pos+=4096;
                        String tmp = new String(buff, "UTF-8");
                        if (tmp.contains(delimiter + "")) {
                            int posi = tmp.indexOf(delimiter + ""); //find first delimiter
                            tmp = tmp.substring(0, posi+1);      //delete util this part
                           // System.out.println(tmp+"==");
                            notfinalBlock = false;
                        }
                        //System.out.println("add");
                        buffer.append(tmp);
                    }
                }//else
                  //  System.out.println("aconteceu");
            }



            if(initial != 0) { //remove the trash before the logical's start
                int posi = buffer.indexOf(delimiter+""); //find first delimiter
                buffer.delete(0,posi+1);                  //delete util this part
            }
            return buffer.toString().replaceAll("[\\p{Cntrl}&&[^\\r\\n\\t]]", "");
        } catch (Exception e) {
            System.out.println("[ERROR] - HDFS.getSplit");
            e.printStackTrace();
        }
        return null;
    }


    public void setfile (String path, long pos)  { // ou um ou outro
        try {
            Path pt=new Path(path);
            fsDataInputStream = fs.open(pt);
            fsDataInputStream.seek(pos);

            reader = new LineReader(fsDataInputStream);
        } catch (IOException e) {
            System.out.println("[ERROR] - HDFS.setfile");
            e.printStackTrace();
        }
        state = pos;
    }


    /* - read_chunck
        @Method: Return a chunck of bytes (byte[])
        @in:     The initial and final block's position, the chunck's size,
                 The block's index, and if the block is the last.
        @out:    Return a chunck of bytes (byte[])
     */
    public byte[] read_chunck( long start, long offset, int size_of_chunck,int index_bloco, boolean last ) throws Exception {
        byte[] chunck = new byte[size_of_chunck];
        if(state == start){




            /*
                Eu sei quantos blocos tem, qual o tamanho dos blocos, entao eu sei onde vou começar a extrair
                FALTA PEGAR O TAMANHO DO BLOCO DINAMICAMENTE
             */
            if(index_bloco!=0){
                int faltantes = index_bloco*67108864 - (size_of_chunck)*((index_bloco*67108864)/size_of_chunck); //divisao por inteiro, entao só obtenho a parte inteira
                int jump = size_of_chunck - faltantes;
                state = state+jump;
            }
        }
        if(last)
            if((state + size_of_chunck ) > (start+offset))
                size_of_chunck = (int) ((start+offset) - state);

        fsDataInputStream.readFully(state, chunck, 0,  size_of_chunck);
        state += size_of_chunck;


        if( state>=(start +offset) )
            this.ended = true;

        return chunck;
    }




    public boolean hasrecord(){   return !ended;   }


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


    public long refreshState (){
        return this.state;
    }

    /*---------------------------------------------------------------------------------------

                                    Write Operations

     ---------------------------------------------------------------------------------------*/



    public void  createFile(String dst, boolean overwrite) throws IOException {
        fs = FileSystem.get(conf);
        Path pt=new Path(dst);
        fs.create(pt, overwrite);
        fs.close();

    }

    public boolean writeFILE (String text, String dst, boolean append)  {
        try {
            fs = FileSystem.get(conf);

            Path pt=new Path(defaultFS+dst);
            System.out.println(defaultFS+dst);
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
