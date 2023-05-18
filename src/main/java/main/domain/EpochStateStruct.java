package main.domain;

import main.CommunicationProtocol;

public class EpochStateStruct {
  int ValTs;
  CommunicationProtocol.Value val;

  public EpochStateStruct() {
    val = CommunicationProtocol.Value.newBuilder().setDefined(false).build();
  }

  public EpochStateStruct(int value, int timestamp) {
    ValTs = timestamp;
    this.val = CommunicationProtocol.Value.newBuilder().setV(value).setDefined(true).build();
  }
  public EpochStateStruct(CommunicationProtocol.Value value, int timestamp) {
    ValTs = timestamp;
    this.val = value;
  }

  public int getValTs() {
    return ValTs;
  }

  public CommunicationProtocol.Value getVal() {
    return val;
  }
}
