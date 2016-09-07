package integration;

import java.io.IOException;

public class Bloco implements java.io.Serializable {

    private long start=0;
    private long end = 0;
    private int index = 0;
    private long state = 0;
    private boolean hasRecords = true;
    private int RecordsReaded = 0;
    private boolean ultimo = false;
    private String path = "";
    private HDFS dfs =  new HDFS("hdfs://localhost:9000");

    public Bloco() {
    }




    public void setStart(long start) {
        this.start = start;
        this.state = start;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public void setTheLast(boolean ultimo) {  this.ultimo = ultimo;   }



    public void setIndex(int index) {    this.index = index;   }



    public String[] getRecords (){
        setHasRecords(false);
        return dfs.getSplit((int) start,(int)end,path,ultimo,'\n').split("\n");
    }

    public void setPath(String path) {
        this.path = path;

        try {
            dfs.setfile(path,this.start);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // public String[] getRecords (){
    //   Handler_HDFS dfs =  new Handler_HDFS("hdfs://localhost:9000");
    // return dfs.getSplit((int) start,(int)end,path,ultimo,'\n').split("\n");
    //}


    public String getRecord (){

        String record = null;
        try {
            record = dfs.getRecord(RecordsReaded,state,(int)end,path,ultimo,'\n');

        } catch (IOException e) {
            //e.printStackTrace();
            record = "";
        }
        state = dfs.refreshState();
        //System.out.println(state);
        RecordsReaded++;
        if( state >= end) {
            setHasRecords(false);
            //System.out.println("CHEGOU AO FIM");
        }
        return record;
    }

    public boolean HasRecords() {
        return hasRecords;
    }

    public void setHasRecords(boolean hasRecords) {
        this.hasRecords = hasRecords;
    }

    public int getRecordsReaded() {return RecordsReaded;}

   // public void setRecordsReaded(int recordsReaded) {
    //    RecordsReaded = recordsReaded;
  //  }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public String getPath() {  return path;    }

    public int getIndex() {   return index; }

    public boolean istheLast() {
        return ultimo;
    }

    public String toString(){
        return "Block"+index+ ": " + start + " "+end;
    }
}
