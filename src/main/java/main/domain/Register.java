package main.domain;

import main.CommunicationProtocol;

import java.util.List;

public class Register {
    int selfRank;
    NNARStruct NNARStruct;
    int acks;
    int writeVal;
    int rid;
    Integer readVal;
    boolean reading;
    String systemId;
    List<CommunicationProtocol.ProcessId> processIds;
    NNARStruct[] readList;

    public Register(int selfRank, NNARStruct NNARStruct, int acks, int writeVal, int rid, int readVal, boolean reading, String systemId, List<CommunicationProtocol.ProcessId> processIds, NNARStruct[] readList) {
        this.selfRank = selfRank;
        this.NNARStruct = NNARStruct;
        this.acks = acks;
        this.writeVal = writeVal;
        this.rid = rid;
        this.readVal = readVal;
        this.reading = reading;
        this.systemId = systemId;
        this.processIds = processIds;
        this.readList = readList;
    }

    public int getSelfRank() {
        return selfRank;
    }

    public void setSelfRank(int selfRank) {
        this.selfRank = selfRank;
    }

    public NNARStruct getValueStruct() {
        return NNARStruct;
    }

    public void setValueStruct(NNARStruct NNARStruct) {
        this.NNARStruct = NNARStruct;
    }

    public int getAcks() {
        return acks;
    }

    public void setAcks(int acks) {
        this.acks = acks;
    }

    public int getWriteVal() {
        return writeVal;
    }

    public void setWriteVal(int writeVal) {
        this.writeVal = writeVal;
    }

    public int getRid() {
        return rid;
    }

    public void setRid(int rid) {
        this.rid = rid;
    }

    public Integer getReadVal() {
        return readVal;
    }

    public void setReadVal(Integer readVal) {
        this.readVal = readVal;
    }

    public boolean isReading() {
        return reading;
    }

    public void setReading(boolean reading) {
        this.reading = reading;
    }

    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    public List<CommunicationProtocol.ProcessId> getProcessIds() {
        return processIds;
    }

    public void setProcessIds(List<CommunicationProtocol.ProcessId> processIds) {
        this.processIds = processIds;
    }

    public NNARStruct[] getReadList() {
        return readList;
    }

    public void setReadList(NNARStruct[] readList) {
        this.readList = readList;
    }
}
