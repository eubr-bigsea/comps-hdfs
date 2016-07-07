package a;

public class Bloco implements java.io.Serializable {

    private long start=0;
    private long end = 0;
    private int index = 0;
    private boolean ultimo = false;
    private String path = "";

    public Bloco() {
    }

    public void setStart(long start) {
        this.start = start;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public void setTheLast(boolean ultimo) {  this.ultimo = ultimo;   }



    public void setIndex(int index) {    this.index = index;   }

    public void setPath(String path) {   this.path = path; }

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
