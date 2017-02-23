package integration;

import java.io.Serializable;

public class Block implements Serializable {

    //private static final long serialVersionUID = 1L;

    private long start=0;
    private long end = 0;
    private int index = 0;
    private long state = 0;
    private boolean hasRecords = true;
    private String defaultFS = ""; //trocar por filepath
    private boolean last_block = false;
    private String path = "";
    private HDFS dfs = null;

    private boolean started = false;

    public Block (){}

    public Block(long start,  long end, boolean last,
                 String defaultFS, String path, int number_block) {
        this.start = start;
        this.state = start;
        this.end = end;
        this.last_block = last;
        this.defaultFS = defaultFS;

        this.index = number_block;
        this.path = path;

    }


        public void setStart(long start)   { this.start = start;  this.state = start;   }
        public void setEnd  (long end)     { this.end = end;    }
        public void setTheLast  (boolean ultimo)    {  this.last_block = ultimo;   }
        public void setIndex    (int index)         {  this.index = index;   }


        public long getStart()   {   return start;  }
        public long getEnd()     {   return end;    }
        public String getPath()  {   return path;   }
        public int getIndex()    {   return index;  }


    public void start_conection(){
        this.dfs = new HDFS(defaultFS);
        this.dfs.setfile(path, this.start);
        this.started = true;
    }

    public boolean HasRecords()  {
        if(!started)
            start_conection();
        return hasRecords;
    }

    public void closeFile(){
        dfs.closeConection();
    }

    public void setHasRecords (boolean hasRecords) {
        this.hasRecords = hasRecords;
        //if (!hasRecords)
        //    closeFile();
    }

    public void setDefaultFS(String defaultFS){
        this.defaultFS = defaultFS;
    }

    public void setPath(String path) {
        //dfs.setfile(path, this.start);
        this.path = path;
    }




    /*
    private ArrayList<String> records = new ArrayList<String>();

    public ArrayList<String> GetRecords (){ // List of ALL RECORDS


      //  setHasRecords(false);

        System.out.println( "AQUI");
        String record = null;
        try {
            records = dfs.readALLLineText(start,end);

        } catch (Exception e) {
            e.printStackTrace();
        }

        state = dfs.refreshState();
        setHasRecords(false);
        return records;
    }

    public String GETRECORD2(){

        if(records.isEmpty())
            records = GetRecords();

        String record = records.get(++RecordsReaded);

        return record;
    }

    public boolean HAS(){
        if(records.isEmpty())
            records = GetRecords();

        return (RecordsReaded<records.size());

    }
    */



    public String[] getRecords (){ // List of ALL RECORDS  ---> ARRUMAR ISSO
        if(!started)
            start_conection();
        String[] split = dfs.getSplit(start, end,path,last_block,'\n').split("\n");
        setHasRecords(false);

        return split;
    }

    /* - getRecord
         @Method:   To read records delimited by "\n".
                    This can be possible while the reader
                    dont arrive at the final byte of the block.
         @in:       Nothing
         @out:      Return a piace of n bytes
    */
    public String getRecord(){
        if(!started){
            start_conection();
            setPath(path);
        }
        String record = null;
        try {
            record = dfs.readLineText(start,end);
        } catch (Exception e) {
            System.out.println("[ERROR] - Block.getRecord()");
            e.printStackTrace();
        }
        state = dfs.refreshState();
        if(!dfs.hasrecord()) {
            setHasRecords(false);
        }
        return record;
    }

    /* - getRecord_Chunck
         @Method: To read records with pattern
         @in:     The size of the piace
         @out:    Return a piace of n bytes
    */
    public byte[] getRecord_Chunck(int size){
        if(!started)
            start_conection();

        byte[] chunck = new byte[size];
        try {
            chunck = dfs.read_chunck(start,end,size,index,last_block);
        } catch (Exception e) {
            System.out.println("[ERROR] - Block.getRecord_Chunck()");
            e.printStackTrace();
        }
        state = dfs.refreshState();

        if(!dfs.hasrecord())
            setHasRecords(false);

        return chunck;
    }


    /*
    public String getRecord (){

        String record = null;
        try {
            record = dfs.getRecord(state,end,path,last_block,'\n');

        } catch (IOException e) {
            //e.printStackTrace();
            record = "";
        }
        state = dfs.refreshState();
        //System.out.println(state);
      //  RecordsReaded++;
        if( state >= end) {
            setHasRecords(false);
            //System.out.println("CHEGOU AO FIM");
        }
        return record;
    }
    */



   // public int getRecordsReaded() {return RecordsReaded;}

   // public void setRecordsReaded(int recordsReaded) {
    //    RecordsReaded = recordsReaded;
  //  }


    // public boolean istheLast() {       return ultimo;    }

    public String toString(){
        return "File: "+path+" | Block "+index+ ": " + start + " Length " + end + " -  Last block: "+last_block ;
    }
}
