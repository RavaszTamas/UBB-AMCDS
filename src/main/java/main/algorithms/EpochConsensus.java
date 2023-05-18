package main.algorithms;

import main.CommunicationProtocol;
import main.domain.EpochStateStruct;
import main.pipelines.PipelineExecutor;
import main.server.QueueProcessor;

import java.util.HashMap;
import java.util.Map;

public class EpochConsensus extends AbstractMessagingAlgorithm {
  String ownerUc;
  QueueProcessor queueProcessor;
  EpochStateStruct epochStateStruct;
  CommunicationProtocol.Value tempVal;
  Map<String, EpochStateStruct> stateStructMap;
  int n;
  int accepted;
  String systemId;
  int epochTs;

  public EpochConsensus(
      String id,
      PipelineExecutor pipelineExecutor,
      EpochStateStruct epochStateStruct,
      QueueProcessor queueProcessor,
      int n,
      int epochTimeStamp,
      String ownerUc) {
    super(id, pipelineExecutor);
    this.queueProcessor = queueProcessor;
    this.epochStateStruct = epochStateStruct;
    this.stateStructMap = new HashMap<>();
    this.ownerUc = ownerUc;
    this.n = n;
    this.accepted = 0;
    this.systemId = this.pipelineExecutor.getSystemId();
    this.epochTs = epochTimeStamp;
    this.tempVal = CommunicationProtocol.Value.newBuilder().setDefined(false).build();
  }

  @Override
  public void send(CommunicationProtocol.Message message) {
    if (message.getType().equals(CommunicationProtocol.Message.Type.EP_PROPOSE)) {
      this.epPropose(message.getEpPropose().getValue().getV());
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.EP_ABORT)) {
      this.epAborted();
    }
  }

  private void epAborted() {

    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.EP_ABORTED)
            .setEpAborted(
                CommunicationProtocol.EpAborted.newBuilder()
                    .setValueTimestamp(this.epochStateStruct.getValTs())
                    .setValue(
                        CommunicationProtocol.Value.newBuilder()
                            .setV(this.epochStateStruct.getVal().getV())
                            .setDefined(this.epochStateStruct.getVal().getDefined())
                            .build())
                    .build())
            .build();
    this.pipelineExecutor.submitToPipeline("app." + ownerUc + "." + this.id + ".beb.pl", message);
  }

  private void epPropose(int v) {

    this.tempVal = CommunicationProtocol.Value.newBuilder().setDefined(true).setV(v).build();

    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.BEB_BROADCAST)
            .setToAbstractionId("app." + ownerUc + "." + this.id + ".beb.pl")
            .setSystemId(this.systemId)
            .setBebBroadcast(
                CommunicationProtocol.BebBroadcast.newBuilder()
                    .setMessage(
                        CommunicationProtocol.Message.newBuilder()
                            .setToAbstractionId("app." + ownerUc + "." + this.id)
                            .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_READ)
                            .setEpInternalRead(
                                CommunicationProtocol.EpInternalRead.newBuilder().build())
                            .build())
                    .build())
            .build();
    this.pipelineExecutor.submitToPipeline("app." + ownerUc + "." + this.id + ".beb.pl", message);
  }

  @Override
  public void deliver(CommunicationProtocol.Message message) {
    if (message.getType().equals(CommunicationProtocol.Message.Type.EP_INTERNAL_READ)) {
      this.plSendState(message.getPlDeliver().getSender());
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.EP_INTERNAL_WRITE)) {
      this.epochStateStruct =
          new EpochStateStruct(this.epochTs, message.getEpInternalWrite().getValue().getV());
      this.sendAccept(message.getPlDeliver().getSender());
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.EP_INTERNAL_DECIDED)) {
      this.deliverDecided(message.getEpInternalDecided().getValue().getV());
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.EP_INTERNAL_STATE)) {
      String key =
          message.getPlDeliver().getSender().getHost()
              + ":"
              + message.getPlDeliver().getSender().getPort();
      int newValTS = message.getEpInternalState().getValueTimestamp();
      CommunicationProtocol.Value newValue = message.getEpInternalState().getValue();
      this.stateStructMap.put(key, new EpochStateStruct(newValue, newValTS));
      this.checkStates();
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.EP_INTERNAL_ACCEPT)) {
      System.out.println("Accepted");
      this.accepted += 1;
      this.checkAccepted();
    }
  }

  private void checkAccepted() {
    if (this.accepted > this.n / 2 + 1) {
      this.accepted = 0;
      System.out.println("Accepted success");
      this.bebBroadcastDecided();
    }
  }

  private void bebBroadcastDecided() {
    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.BEB_BROADCAST)
            .setToAbstractionId("app." + ownerUc + "." + this.id + ".beb.pl")
            .setSystemId(this.systemId)
            .setBebBroadcast(
                CommunicationProtocol.BebBroadcast.newBuilder()
                    .setMessage(
                        CommunicationProtocol.Message.newBuilder()
                            .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_DECIDED)
                            .setToAbstractionId("app." + ownerUc + "." + this.id)
                            .setEpInternalDecided(
                                CommunicationProtocol.EpInternalDecided.newBuilder()
                                    .setValue(
                                        CommunicationProtocol.Value.newBuilder()
                                            .setV(this.tempVal.getV())
                                            .setDefined(true)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();

    this.pipelineExecutor.submitToPipeline("app." + ownerUc + "." + this.id + ".beb.pl", message);
  }

  private void checkStates() {
    int counter = 0;
    for (var stateStruct : this.stateStructMap.values()) {
      if (stateStruct != null) {
        counter++;
      }
    }
    if (counter > this.n / 2 + 1) {
      EpochStateStruct highest = this.highestState();
      if (highest.getVal().getDefined()) {
        this.tempVal =
            CommunicationProtocol.Value.newBuilder()
                .setV(highest.getVal().getV())
                .setDefined(highest.getVal().getDefined())
                .build();
      }
      this.stateStructMap = new HashMap<>();

      CommunicationProtocol.Message message =
          CommunicationProtocol.Message.newBuilder()
              .setType(CommunicationProtocol.Message.Type.BEB_BROADCAST)
              .setToAbstractionId("app." + ownerUc + "." + this.id + ".beb.pl")
              .setSystemId(this.systemId)
              .setBebBroadcast(
                  CommunicationProtocol.BebBroadcast.newBuilder()
                      .setMessage(
                          CommunicationProtocol.Message.newBuilder()
                              .setToAbstractionId("app." + ownerUc + "." + this.id)
                              .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_WRITE)
                              .setEpInternalWrite(
                                  CommunicationProtocol.EpInternalWrite.newBuilder()
                                      .setValue(
                                          CommunicationProtocol.Value.newBuilder(this.tempVal)
                                              .build())
                                      .build())
                              .build())
                      .build())
              .build();
      this.pipelineExecutor.submitToPipeline("app." + ownerUc + "." + this.id + ".beb.pl", message);
    }
  }

  private EpochStateStruct highestState() {
    EpochStateStruct highestStateStruct = null;
    for (var state : this.stateStructMap.values()) {
      if (state != null) {
        highestStateStruct = state;
        break;
      }
    }
    for (var state : this.stateStructMap.values()) {
      if (state != null && highestStateStruct != null) {
        if (state.getValTs() > highestStateStruct.getValTs()) highestStateStruct = state;
      }
    }
    return highestStateStruct;
  }

  private void deliverDecided(int v) {
    System.out.println("SENDING DECIDED");
    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setToAbstractionId("app." + ownerUc + "." + this.id + ".pl")
            .setType(CommunicationProtocol.Message.Type.EP_DECIDE)
            .setEpDecide(
                CommunicationProtocol.EpDecide.newBuilder()
                    .setEts(this.epochStateStruct.getValTs())
                    .setValue(
                        CommunicationProtocol.Value.newBuilder().setV(v).setDefined(true).build())
                    .build())
            .build();
    this.pipelineExecutor.submitToPipeline("app." + ownerUc + "." + this.id + ".pl", message);
  }

  private void sendAccept(CommunicationProtocol.ProcessId dest) {
    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.PL_SEND)
            .setToAbstractionId("app." + ownerUc + "." + this.id + ".pl")
            .setSystemId(this.systemId)
            .setPlSend(
                CommunicationProtocol.PlSend.newBuilder()
                    .setDestination(dest)
                    .setMessage(
                        CommunicationProtocol.Message.newBuilder()
                            .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_ACCEPT)
                            .setToAbstractionId("app." + ownerUc + "." + this.id)
                            .setEpInternalAccept(
                                CommunicationProtocol.EpInternalAccept.newBuilder().build())
                            .build())
                    .build())
            .build();
    this.pipelineExecutor.submitToPipeline("app." + ownerUc + "." + this.id + ".pl", message);
  }

  private void plSendState(CommunicationProtocol.ProcessId receiver) {
    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.PL_SEND)
            .setToAbstractionId("app." + ownerUc + "." + this.id + ".pl")
            .setSystemId(this.systemId)
            .setPlSend(
                CommunicationProtocol.PlSend.newBuilder()
                    .setDestination(receiver)
                    .setMessage(
                        CommunicationProtocol.Message.newBuilder()
                            .setType(CommunicationProtocol.Message.Type.EP_INTERNAL_STATE)
                            .setToAbstractionId("app." + ownerUc + "." + this.id)
                            .setEpInternalState(
                                CommunicationProtocol.EpInternalState.newBuilder()
                                    .setValue(
                                        CommunicationProtocol.Value.newBuilder()
                                            .setV(this.epochStateStruct.getVal().getV())
                                            .setDefined(this.epochStateStruct.getVal().getDefined())
                                            .build())
                                    .setValueTimestamp(this.epochStateStruct.getValTs())
                                    .build())
                            .build())
                    .build())
            .build();
    this.pipelineExecutor.submitToPipeline("app." + ownerUc + "." + this.id + ".pl", message);
  }
}
