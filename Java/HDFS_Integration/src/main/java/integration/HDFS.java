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
    private BufferedWriter br;


    public HDFS(String defaultFS){
        this.defaultFS = defaultFS;
        this.conf = new Configuration();
        this.conf.set("fs.defaultFS", defaultFS);
        this.conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
        this.conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
        this.conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        this.conf.setBoolean("fs.file.impl.disable.cache", true);

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
     * The user should not use this method.
     *
     * @deprecated
     */
    public ArrayList<BlockLocality> findALLBlocksByStorageAPI (String path,int number_block){
        path = defaultFS+path;
        if (debug)
            System.out.println("Current Path: " + path);
        ArrayList<BlockLocality> b = new ArrayList<BlockLocality>();
        long first_offset = 0;

        try{

            FileStatus Status;
            BlockLocation[] bLocations;
            Path file = new Path(path);
            FileStatus fStatus = fs.getFileStatus(file);
            Status=fStatus;
            int baseNumber = number_block;
            bLocations= fs.getFileBlockLocations(Status, 0, Status.getLen());

            for(BlockLocation aLocation : bLocations){

                BlockLocality c = new BlockLocality();
                c.setStart(aLocation.getOffset());
                c.setEnd(aLocation.getLength());
                c.setDefaultFS(defaultFS);

                if ((number_block +1) == (baseNumber+bLocations.length) )  c.setTheLast(true);
                else                                                       c.setTheLast(false);

                c.setPath(path);
                c.setIndex(number_block);
                if (number_block == 0)
                    first_offset = aLocation.getLength();
                c.setFragment_size(first_offset);
                c.setLocations(aLocation.getHosts());

                number_block++;

                b.add(c);


            }

        } catch (Exception e) {
            System.out.println("[ERROR] - HDFS.findALLBlocksByStorageAPI()");
            e.printStackTrace();
        }

        return b;
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
                c.setLocations(aLocation.getHosts());

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

            if (debug) System.out.println("size: " + size + " BlockSize:"+blockSize);

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
                c.setLocations(new String[]{});
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

    /**
     *
     * @deprecated
     */
    public boolean hasrecord(){   return !ended;   }

    /**
     *
     * @deprecated
     */
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
     *
     * @deprecated
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

    /**
     *
     * @param  path  Path of the file
     * @param  pos   Position in the current file to seek
     *
     * @deprecated
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
    public byte[] read_chunck( long start, long offset, int size_of_chunck,
                               int i_block, boolean the_last ) throws Exception {
        byte[] chunck = new byte[size_of_chunck];

        if(state == start){

            if(i_block!=0){
                long faltantes = (i_block * fragment_size) - ( size_of_chunck *
                        ((i_block* fragment_size)/size_of_chunck) );

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


    /*---------------------------------------------------------------------------

                                    Write Operations

     --------------------------------------------------------------------------*/

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
     * Write the text in a HDFS file.
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


    /**
     * Write the text in a HDFS file.
     *
     * @param  text         The content to be written
     * @param  dst          Path of the file
     * @param  first        Whether is the first block to be written
     * @param  last         Whether is the last  block to be written
     */
    public boolean writeBlocks (String text, String dst, boolean first, boolean last)  {
        try {

            if (first){
                fs = FileSystem.get(conf);
                Path pt = new Path(defaultFS+dst);
                OutputStreamWriter out =
                        new OutputStreamWriter(fs.create(pt, true));
                br = new BufferedWriter(out);
            }

            br.write(text);

            if (last)
                br.close();

            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

}
