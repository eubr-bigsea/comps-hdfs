package integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.LineReader;

import java.io.*;
import java.util.ArrayList;


public class HDFS implements Serializable {

    //private static final long serialVersionUID = 1L;

    private Configuration conf;
    private long fragment_size = 0;
    private boolean debug =false;
    private Path path = null;
    private String defaultFS = "";

    private FileSystem fs;
    private FSDataInputStream fsDataInputStream;
    private LineReader reader = null;

    private boolean ended = false;
    private long state=0;
    private long bytes_readeds = 0;



    public HDFS(String defaultFS){
        this.defaultFS = defaultFS;
        this.conf = new Configuration();
        this.conf.set("fs.defaultFS", defaultFS);
        this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
        this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
        this.conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        this.conf.setBoolean("fs.file.impl.disable.cache", true);


        //  this.conf.set("dfs.replication", "1");          //ONLY IF IS SINGLE NODE

        try{
            this.fs = FileSystem.newInstance(conf);


        } catch (Exception e) {
            System.out.println("Error: HDFS's constructor");
            e.printStackTrace();
        }
    }



    /*---------------------------------------------------------------------------------------

                                    Management Operations

     ---------------------------------------------------------------------------------------*/


    /**
     * Set the user who is accessing the data
     *
     * @param  user  username
     */
    public void setUserHDFS(String user){
        this.conf.set("hadoop.job.ugi", user);
    }


    /**
     * List all the files in the folder
     *
     * @param  dir  Path of the folder
     * @return      Return a list of all files in the folder
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


    /**
     * Create a new folder on the HDFS. You need to create the folders in order.
     *
     * @param  path  Path of the folder
     */
    public void mkdir (String path){
        try{
            fs.createNewFile(new Path(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns a list of blocks of a file in the HDFS
     *
     * @param  path  Path of the file in the HDFS
     * @return       List of blocks splitted by number of the real blocks in the HDFS
     */
    public ArrayList<Block> findALLBlocks (String path){
        path = defaultFS+path;
        if (debug)
            System.out.println("Current Path: " + path);
        ArrayList<Block> b = new ArrayList<Block>();
        long first_offset = 0;

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
                if (number_block == 0)
                    first_offset = aLocation.getLength();
                c.setFragment_size(first_offset);

                number_block++;

                b.add(c);


            }

        } catch (Exception e) {
            System.out.println("[ERROR] - HDFS.findALLBlocks()");
            e.printStackTrace();
        }

        return b;
    }


    /**
     * Returns a list of n blocks of a file in the HDFS
     *
     * @param  path  Path of the file in the HDFS
     * @param  nodes Number of blocks wanterd
     * @return       List of blocks splitted by number of nodes
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
            long first_offset = 0;

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

                if (number_block == 0)
                    first_offset = offset;
                c.setFragment_size(first_offset);

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
        IOUtils.closeStream(fsDataInputStream);
    }

    public boolean hasrecord(){   return !ended;   }

    public long refreshState (){
        return this.state;
    }

    public void setDebug(boolean d){
        debug = d;
    }


    /*---------------------------------------------------------------------------------------

                                    Read Operations

     ---------------------------------------------------------------------------------------*/




    /**
     * Responsible to  return the next record (delimited by "\n" or "\r\n") in the current Block.
     *
     * @param  start    Position of first byte in the current Block
     * @param  offset   Length of the current Block
     * @param  last     whether the block is the last block of a file or not
     * @return          A record
     */

    public String  readLineText( long start, long offset, boolean last ) throws Exception {
        String split = null;
        Text buffer =  new Text();

        try {
            if(bytes_readeds ==0) {
                if(start !=  0)
                    bytes_readeds+=reader.readLine(buffer);  //remove the trash of the previous block;
            }

            bytes_readeds+= reader.readLine(buffer);
            split = buffer.toString();

            if (last){
                if  (bytes_readeds >= offset )
                    this.ended = true;
            } else if (bytes_readeds > offset )
                this.ended = true;

        } catch (EOFException e) {
            System.out.println("[ERROR] - HDFS.readLineText()");
        }

        return split;

    }


//    public String getSplit (long initial, long length, String path, boolean last, char delimiter){
//
//        try{
//            Path pt=new Path(path);
//            FSDataInputStream fsDataInputStream = fs.open(pt);
//            long pos =  0;
//            if (debug)
//                System.out.println("HDFS.getSplit = ["+initial + " "+ length+"]" );
//
//            //Copy all block
//            StringBuffer buffer = new StringBuffer();
//            if(initial ==0) { // is the first block
//                byte[] buff = new byte[(int) length];
//                fsDataInputStream.read(initial, buff, 0, (int) length);
//                buffer.append(new String(buff, "UTF-8"));
//                pos =  initial+length;
//            }else{ //is not the first block
//                byte[] buff = new byte[(int) length+2];
//                fsDataInputStream.read(initial-1, buff, 0, (int) length);
//                buffer.append(new String(buff, "UTF-8"));
//                pos =  initial+length-1;
//            }
//
//            //Find the logical end of this text stretch
//            boolean notfinalBlock = true;
//            if(!last) {
//                if (!buffer.substring(buffer.length()-1).equals(delimiter+"")) { //verify if is ended
//                    int offset2 = 256;
//                    byte[] buff = new byte[offset2];
//                    while (notfinalBlock) {
//                        fsDataInputStream.read(pos, buff, 0, offset2);
//                        pos+=offset2;
//                        String tmp = new String(buff, "UTF-8");
//                        if (tmp.contains(delimiter + "")) {
//                            int posi = tmp.indexOf(delimiter + ""); //find first delimiter
//                            tmp = tmp.substring(0, posi+1);      //delete util this part
//                            notfinalBlock = false;
//                        }
//                        buffer.append(tmp);
//                    }
//                }
//            }
//
//            if(initial != 0) {
//                int posi = buffer.indexOf(delimiter+""); //find first delimiter
//                buffer.delete(0,posi+1);                  //delete util this part
//            }
//
//            return buffer.toString().replaceAll("[\\p{Cntrl}&&[^\\r\\n\\t]]", "");
//        } catch (Exception e) {
//            System.out.println("[ERROR] - HDFS.getSplit");
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//
//    /* - readALLFiletoString
//            @Method: Return all the file in a String
//            @in:     Path of the file
//            @out:    String with all the contents of the file
//
//        Não é necessario, basta criar um block unico
//    */
//    public String readALLFiletoString (String path){
//        String text ="";
//        BufferedReader br =null;
//        Path pt=new Path(path);
//
//        try{
//            FSDataInputStream fsDataInputStream = fs.open(pt);
//            br = new BufferedReader(new InputStreamReader(fsDataInputStream));
//
//            String line;
//            line=br.readLine();
//            while (line != null){
//                text +=line+"\n";
//                line=br.readLine();
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return text;
//    }


    /**
     * Returns a list of records in the current Block
     *
     * @param  path  Path of the file
     * @param  pos   Position in the current file to seek
     */
    public void setfile (String path, long pos)  { // ou um ou outro
        try {
            Path pt=new Path(path);
            fsDataInputStream = fs.open(pt);
            fsDataInputStream.seek(pos);
            bytes_readeds = 0;
            reader = new LineReader(fsDataInputStream);
            state = pos;
        } catch (IOException e) {
            System.out.println("[ERROR] - HDFS.setfile");
            e.printStackTrace();
        }

    }


    /**
     * Returns a chunck of bytes (byte[])
     *
     * @param start             Initial position
     * @param offset            Length to read
     * @param size_of_chunck    The chunck's size
     * @param i_block       The block's index
     * @param the_last              Whether the block is the last or not
     * @return                  A chunck of "size_of_chunck" bytes (byte[])
     */
    public byte[] read_chunck( long start, long offset, int size_of_chunck,int i_block, boolean the_last ) throws Exception {
        byte[] chunck = new byte[size_of_chunck];

        if(state == start){

            /*
                Eu sei quantos blocos tem, qual o tamanho dos blocos, entao eu sei onde
                vou começar a extrair
             */
            if(i_block!=0){
                long faltantes = i_block*fragment_size -
                               (size_of_chunck)*((i_block* fragment_size)/size_of_chunck); //divisao por inteiro, entao só obtenho a parte inteira
                int jump = size_of_chunck - (int) faltantes;
                state = state+jump;
            }
        }

        if(the_last)
            if((state + size_of_chunck ) > (start+offset))
                size_of_chunck = (int) ((start+offset) - state);

        fsDataInputStream.readFully(state, chunck, 0,  size_of_chunck);
        state += size_of_chunck;


        if( state>=(start +offset) )
            this.ended = true;

        return chunck;
    }


//    public String getRecord (int RecordsReadeds, long start, int end, String path, boolean last, char delimiter) throws IOException {
//        StringBuffer buffer = new StringBuffer();
//
//        try{
//
//            byte ch;
//            boolean notfinalBlock = true;
//            buffer = new StringBuffer();
//
//            long pos = start;
//
//            if(RecordsReadeds == 0) { //remove the trash
//
//                if(pos !=0) {
//                    do {
//                        ch = fsDataInputStream.readByte();  //read before counting position
//                        pos++; //position tracker
//                    }while (ch != (byte) delimiter);
//
//                }
//            }
//
//            while (notfinalBlock){
//                ch = fsDataInputStream.readByte();  //read before counting position
//                pos++; //position tracker
//                // System.out.println(ch);
//                if(ch==((byte) delimiter)){
//                    notfinalBlock = false;
//                }else{
//                    if(pos != (start+end) )
//                        buffer.append((char)ch);
//                }
//
//                if(last && (pos >= end))
//                    notfinalBlock = false;
//
//            }
//
//            //System.out.println("SPLIT: "+ buffer.toString());
//            state = (int) pos;
//            // System.out.println("State: "+state);
//
//
//        } catch (Exception e) {
//            System.out.println("Error: readBlock");
//            e.printStackTrace();
//        }
//        return buffer.toString();
//    }




    /*---------------------------------------------------------------------------------------

                                    Write Operations

     ---------------------------------------------------------------------------------------*/

    /**
     * Create a empty file in the HDFS
     *
     * @param  dst          Path of the file
     * @param  overwrite    Overwrite the file whether exists or not
     */
    public void  createFile(String dst, boolean overwrite) throws IOException {
        fs = FileSystem.get(conf);
        Path pt=new Path(dst);
        fs.create(pt, overwrite);
        fs.close();

    }

    /**
     * Create a empty file in the HDFS
     *
     * @param  text         The content to be written
     * @param  dst          Path of the file
     * @param  append       Whether append the content into the file (if exists) or not
     */
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
