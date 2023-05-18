package main.algorithms;

import main.CommunicationProtocol;
import main.domain.EpochLeader;
import main.domain.EpochStateStruct;
import main.pipelines.PipelineExecutor;
import main.server.QueueProcessor;
import main.utils.Utils;

import java.util.Comparator;
import java.util.List;

public class UniformConsensus extends AbstractMessagingAlgorithm {
  private final QueueProcessor queueProcessor;
  String systemId;
  CommunicationProtocol.Value value;
  int numberOfProcesses;
  boolean proposed;
  boolean decided;
  String pipelineId;

  EpochLeader actualEpochleader;
  EpochLeader newEpochLeader;
  CommunicationProtocol.ProcessId self;
  boolean initDone = false;

  public UniformConsensus(
      String id,
      PipelineExecutor pipelineExecutor,
      QueueProcessor queueProcessor,
      String pipelineId,
      int n) {
    super(id, pipelineExecutor);
    this.systemId = this.pipelineExecutor.getSystemId();
    this.queueProcessor = queueProcessor;
    this.numberOfProcesses = n;
    this.decided = false;
    this.proposed = false;
    this.pipelineId = pipelineId;
  }

  public void init() {
    if (initDone) return;
    this.initDone = true;
    this.value = CommunicationProtocol.Value.newBuilder().setDefined(false).build();
    List<CommunicationProtocol.ProcessId> systems =
        this.queueProcessor.getSystems().get(this.systemId);
    CommunicationProtocol.ProcessId maxProcess =
        systems.stream()
            .max(Comparator.comparingInt(CommunicationProtocol.ProcessId::getRank))
            .orElseThrow();
    this.actualEpochleader = new EpochLeader(0, maxProcess);
    this.newEpochLeader = new EpochLeader(0, null);
    this.self = this.queueProcessor.getOwnerProcess();

    String uniqueDepKey = this.id + ".ec";
    EpochChange epochChange = new EpochChange(id, pipelineExecutor, queueProcessor, pipelineId);
    this.pipelineExecutor.getUniqueLayerCache().put(uniqueDepKey, epochChange);
    this.addNewEpochConcensus(
        0,
        maxProcess,
        new EpochStateStruct(
            CommunicationProtocol.Value.newBuilder().setDefined(false).setV(0).build(), 0));
  }

  void addNewEpochConcensus(
      int ets, CommunicationProtocol.ProcessId leader, EpochStateStruct state) {

    String epID = "ep[" + ets + "]";
    String uniqueDepKey = this.id + "." + epID;
    EpochConsensus epochConsensus =
        new EpochConsensus(
            epID,
            this.pipelineExecutor,
            state,
            queueProcessor,
            this.numberOfProcesses,
            ets,
            this.id);
    if (this.pipelineExecutor.getUniqueLayerCache().containsKey(uniqueDepKey)) {
      this.pipelineExecutor.getLayerCache().remove(epID);
      this.pipelineExecutor.getUniqueLayerCache().remove(uniqueDepKey);
      this.pipelineExecutor
          .getAbstractionPipelineMap()
          .remove("app." + this.id + "." + epID + ".beb.pl");
      this.pipelineExecutor
          .getAbstractionPipelineMap()
          .remove("app." + this.id + "." + epID + ".pl");
    }
    this.pipelineExecutor.getUniqueLayerCache().put(uniqueDepKey, epochConsensus);
    String pipelineID = "app." + this.id + "." + epID + ".beb.pl";
    this.pipelineExecutor.generatePipeline(pipelineID);
    pipelineID = "app." + this.id + "." + epID + ".pl";
    this.pipelineExecutor.generatePipeline(pipelineID);
  }

  @Override
  public void send(CommunicationProtocol.Message message) {
    if (message.getType().equals(CommunicationProtocol.Message.Type.UC_PROPOSE)) {
      this.value = message.getUcPropose().getValue();
      this.checkLeader();
    }
  }

  @Override
  public void deliver(CommunicationProtocol.Message message) {
    if (message.getType().equals(CommunicationProtocol.Message.Type.EC_START_EPOCH)) {
      this.newEpochLeader =
          new EpochLeader(
              message.getEcStartEpoch().getNewTimestamp(),
              message.getEcStartEpoch().getNewLeader());
      this.sendEpEtsAbort();
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.EP_ABORTED)) {
      int ts = Integer.parseInt(Utils.getRegisterIdFromAbstraction(message.getFromAbstractionId()));
      if (ts == this.actualEpochleader.getEpochTs()) {
        this.actualEpochleader =
            new EpochLeader(
                this.newEpochLeader.getEpochTs(),
                CommunicationProtocol.ProcessId.newBuilder(this.newEpochLeader.getProcessId())
                    .build());
        this.checkLeader();
        this.proposed = false;
        this.addNewEpochConcensus(
            this.actualEpochleader.getEpochTs(),
            CommunicationProtocol.ProcessId.newBuilder(this.actualEpochleader.getProcessId())
                .build(),
            new EpochStateStruct(
                message.getEpAborted().getValue(), message.getEpAborted().getValueTimestamp()));
      }
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.EP_DECIDE)) {
      System.out.println(message);
      int ts = Integer.parseInt(Utils.getEpConsensusId(message.getToAbstractionId()));
      System.out.println("aaaaaaaaaaaa");
      System.out.println(decided);
      System.out.println(ts + " " + this.actualEpochleader.getEpochTs());
      System.out.println("bbbbbbbbbbbb");

      if (ts == this.actualEpochleader.getEpochTs() && !decided) {
        this.decided = true;
        CommunicationProtocol.Message payloadMessage =
            CommunicationProtocol.Message.newBuilder()
                .setToAbstractionId("app.pl")
                .setType(CommunicationProtocol.Message.Type.UC_DECIDE)
                .setSystemId(this.systemId)
                .setUcDecide(
                    CommunicationProtocol.UcDecide.newBuilder()
                        .setValue(
                            CommunicationProtocol.Value.newBuilder(message.getEpDecide().getValue())
                                .build())
                        .build())
                .build();
        System.out.println(payloadMessage);
        System.out.println("Sending uc decide to app.pl");
        this.pipelineExecutor.submitToPipeline("app.pl", payloadMessage);
      }
    }
  }

  private void checkLeader() {
    if (Utils.areEqualProcesses(this.actualEpochleader.getProcessId(), this.self)
        && this.value.getDefined()
        && !this.proposed) {
      this.proposed = true;
      this.sendPropose();
    }
  }

  private void sendPropose() {
    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.EP_PROPOSE)
            .setToAbstractionId(
                "app." + this.id + ".ep[" + this.actualEpochleader.getEpochTs() + "]")
            .setSystemId(this.systemId)
            .setEpPropose(
                CommunicationProtocol.EpPropose.newBuilder()
                    .setValue(
                        CommunicationProtocol.Value.newBuilder()
                            .setV(this.value.getV())
                            .setDefined(this.value.getDefined())
                            .build())
                    .build())
            .build();

    this.pipelineExecutor.submitToPipeline(this.pipelineId, message);
  }

  private void sendEpEtsAbort() {
    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.EP_ABORT)
            .setToAbstractionId(
                "app." + this.id + ".ep[" + this.actualEpochleader.getEpochTs() + "]")
            .setSystemId(this.systemId)
            .setEpAbort(CommunicationProtocol.EpAbort.newBuilder().build())
            .build();
    this.pipelineExecutor.submitToPipeline(this.pipelineId, message);
  }
}
