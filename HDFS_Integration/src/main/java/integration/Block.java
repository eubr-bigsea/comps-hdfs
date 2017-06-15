package integration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import storage.StubItf;

public class Block implements Serializable, Cloneable{

    //private static final long serialVersionUID = 1L;

    //Permanent variables of this block
    private long    START         = 0;
    private long    end           = 0;
    private int     index         = 0;
    private String  defaultFS     = "";
    private boolean last_block    = false;
    private long    fragment_size = 0;
    private String  path          = "";
    private String[] locations;


    //Transient variables of ths block
    private boolean started = false;
    private long state = 0;
    private boolean hasRecords = true;
    private HDFS dfs = null;

    public Block (){}

    public void setStart(long start)            { this.START = start;  this.state = start;   }
    public void setEnd  (long end)              { this.end = end;    }
    public void setTheLast  (boolean ultimo)    { this.last_block = ultimo;   }
    public void setIndex    (int index)         { this.index = index;   }
    public void setFragment_size (long f)       { this.fragment_size = f;}
    public void setDefaultFS(String defaultFS)  { this.defaultFS = defaultFS; }
    public void setHasRecords (boolean hasRecords)  { this.hasRecords = hasRecords;   }
    public void setPath(String path)                { this.path = path;   }
    public void setLocations(String[] locs)         {this.locations = locs;}


    public long     getStart()          {   return START;  }
    public long     getEnd()            {   return end;    }
    public String   getPath()           {   return path;   }
    public int      getIndex()          {   return index;  }
    public long     getFragment_size()  {   return fragment_size;}
    public String[] getLocations()      {   return locations;}



    private void start_conection(){
        this.dfs = new HDFS(defaultFS);
        this.dfs.setfile(path, this.START);
        this.state = 0;
        this.started = true;
    }

    public boolean HasRecords() {
        if(!started)
            start_conection();

        return hasRecords;
    }


    public void restart(){
        setHasRecords(true);
        start_conection();
    }

    //you can closeBlock manually
    public void closeBlock(){
        dfs.closeConection();
    }


    public void SetDebug(boolean t){
        dfs.setDebug(t);
    }


    @Override
    public Object clone() {
        Block blk = new Block();
        blk.setStart(this.getStart());
        blk.setEnd(this.getEnd());
        blk.setDefaultFS(defaultFS);
        blk.setTheLast(this.last_block);
        blk.setPath(this.getPath());
        blk.setIndex(this.getIndex());
        blk.setFragment_size(this.getFragment_size());
        blk.setLocations(this.locations);

        return blk;
    }


    /**
     * To read records delimited by "\n" or "\r\n". This can be possible while the reader
     * doesn't arrive at the final byte of the block.
     *
     * @return   A record in String format.
     */
    public String getRecord(){
        if(!started){
            start_conection();
        }
        String record = null;
        try {
            record = dfs.readLineText(START,end,last_block);
        } catch (Exception e) {
            System.out.println("[ERROR] - Block.getRecord()");
            e.printStackTrace();
        }
        state = dfs.refreshState();
        if(!dfs.hasrecord()) {
            setHasRecords(false);
            closeBlock();
        }
        return record;
    }

    /**
     * Returns a list of records in the current Block
     *
     * @return            List of records in the current Block in String format
     */
    public String[] getRecords (){
        // In the getRecords() the extration starts at the init of the block
        if(!started){
            start_conection();
        }

        ArrayList<String> tmp  = new ArrayList<String>();
        while (hasRecords){
            tmp.add(getRecord());
        }

        String[] split = new String[tmp.size()];
        split = tmp.toArray(split);

        return split;
    }

    /**
     * Returns a array of n bytes
     *
     * @param   size    length of the array
     * @return          Returns a array of n bytes
     */
    public byte[] getRecord_Chunck(int size){
        if(!started){
            start_conection();
          //  setPath(path);
        }

        byte[] chunck = new byte[size];
        try {
            chunck = dfs.read_chunck(START,end,size,index,last_block);
        } catch (Exception e) {
            System.out.println("[ERROR] - Block.getRecord_Chunck()");
            e.printStackTrace();
        }

        state = dfs.refreshState();
        if(!dfs.hasrecord()) {
            setHasRecords(false);
            closeBlock();
        }
        return chunck;
    }


    /**
     * Returns a info of the current block.
     *
     * @return   Inform the filename, the id, the initial position, the length, the fragmentation of
     *           the blocks and whether is the last block or not.
     */
    public String toString(){
        String tmp = (last_block)? "Yes":"No";
        String nodes = "";
        for (int i = 0; i<locations.length;i++)
            nodes+=locations[i];

        return  "<<File: "+path+" - Block id_"+index+ ": Start " + START +
                " Length "+ end + " Fragment size:" + fragment_size +
                " - Last block: "+ tmp + " - in: "+nodes +">>";

    }

    /**
     * Returns a info of the current block. Used in the Python Integration.
     *
     * @return   Inform the filename, the id, the initial position, the length, the fragmentation of
     *           the blocks and whether is the last block or not.
     * @deprecated
     */
    public String[] getConfBlock(){
        String[] config = new String[7];
        config[0] = defaultFS;                  //address master hdfs
        config[1] = this.getPath();             //filename
        config[2] = this.getIndex()+"";         //number of the block
        config[3] = this.getStart()+"";         //initial position
        config[4] = this.getEnd()+"";           //length of the block
        config[5] = this.last_block+"";         //whether is the last block or not
        config[6] = this.getFragment_size()+""; //the fragmantation of the file

        return config;
    }


    /**
     * Set this block with the settings passed by parameter
     *
     * @deprecated 
     */
    public void setConfBlock(String[] config) {

        defaultFS     = config[0];
        path          = config[1];
        index         = Integer.parseInt(config[2]);
        START         = Long.parseLong(config[3]);
        end           = Long.parseLong(config[4]);
        last_block    = Boolean.parseBoolean(config[5]);
        fragment_size = Long.parseLong(config[6]);
    }


    //-------------------------------------------//
    // StubItf:

    /**
     * @deprecated 
     */
    public void deletePersistent(){
        //HDFS will handle it
    }

    /**
     * @deprecated 
     */
    public void makePersistent(String id){
        //HDFS will handle it
    }

    /**
     * @deprecated 
     */
    public String getID(){
        //Its important to COMPSs
        return index+"";
    }


    //-------------------------------------------//




}
