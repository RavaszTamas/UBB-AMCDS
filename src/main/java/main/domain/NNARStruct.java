package main.domain;

public class NNARStruct {
    private int ts;
    private int wr;
    private Integer val;

    public NNARStruct() {
        this.ts = 0;
        this.wr = 0;
        this.val = null;
    }
    public NNARStruct(int ts, int wr, Integer val) {
        this.ts = ts;
        this.wr = wr;
        this.val = val;
    }

    public int getTs() {
        return ts;
    }

    public void setTs(int ts) {
        this.ts = ts;
    }

    public int getWr() {
        return wr;
    }

    public void setWr(int wr) {
        this.wr = wr;
    }

    public Integer getVal() {
        return val;
    }

    public void setVal(Integer val) {
        this.val = val;
    }
}
