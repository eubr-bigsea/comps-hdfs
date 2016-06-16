package a;

public class Bloco {

    private long start=0;
    private long end = 0;
    private boolean ultimo = false;
    public Bloco(long start, long end, boolean ultimo) {
        this.start = start;
        this.end = end;
        this.ultimo = ultimo;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public boolean istheLast() {
        return ultimo;
    }

    public String toString(){
        return ""+start + " "+end;
    }
}
