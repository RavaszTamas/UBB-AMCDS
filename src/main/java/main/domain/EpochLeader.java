package main.domain;

import main.CommunicationProtocol;

public class EpochLeader {
  CommunicationProtocol.ProcessId processId;
  int epochTs;

  public EpochLeader(int epochTs, CommunicationProtocol.ProcessId processId) {
    this.processId = processId;
    this.epochTs = epochTs;
  }

  public CommunicationProtocol.ProcessId getProcessId() {
    return processId;
  }

  public int getEpochTs() {
    return epochTs;
  }
}
