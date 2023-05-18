package main.algorithms;

import main.CommunicationProtocol;
import main.pipelines.PipelineExecutor;
import main.server.QueueProcessor;
import main.utils.MessageComposer;
import main.utils.Utils;

import javax.swing.text.Utilities;
import java.util.Comparator;
import java.util.List;

public class EpochChange extends AbstractMessagingAlgorithm {
  CommunicationProtocol.ProcessId trusted;
  CommunicationProtocol.ProcessId self;
  QueueProcessor queueProcessor;
  private int lastTTS;
  private int TS;
  private String systemId;
  private String pipelineId;
  private int numberOfProcesses;

  public EpochChange(
      String id,
      PipelineExecutor pipelineExecutor,
      QueueProcessor queueProcessor,
      String pipelineId) {
    super(id, pipelineExecutor);
    this.systemId = this.pipelineExecutor.getSystemId();
    this.pipelineId = pipelineId;
    this.queueProcessor = queueProcessor;
    this.lastTTS = 0;

    List<CommunicationProtocol.ProcessId> systems =
        this.queueProcessor.getSystems().get(this.systemId);
    CommunicationProtocol.ProcessId maxProcess =
        systems.stream()
            .max(Comparator.comparingInt(CommunicationProtocol.ProcessId::getRank))
            .orElseThrow();

    this.numberOfProcesses = systems.size();
    this.trusted = maxProcess;
    this.self = this.queueProcessor.getOwnerProcess();
    this.TS = this.self.getRank();
  }

  @Override
  public void send(CommunicationProtocol.Message message) {

    this.pipelineExecutor.submitToPipeline(this.pipelineId, message);
  }

  @Override
  public void deliver(CommunicationProtocol.Message message) {
    if (message.getType().equals(CommunicationProtocol.Message.Type.ELD_TRUST)) {
      this.trusted = message.getEldTrust().getProcess();
      if (Utils.areEqualProcesses(this.self, this.trusted)) {
        this.TS += 1;
        this.bebBroadcastNewEpoch();
      }
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.EC_INTERNAL_NEW_EPOCH)) {
      CommunicationProtocol.ProcessId sender = message.getPlDeliver().getSender();
      int newTs = message.getEcInternalNewEpoch().getTimestamp();
      if (Utils.areEqualProcesses(sender, this.trusted) && newTs > this.lastTTS) {
        this.lastTTS = newTs;
        this.startEpoch(newTs);
      } else {
        this.sendNack(message.getPlDeliver().getSender(), message.getToAbstractionId());
      }
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.EC_INTERNAL_NACK)) {
      if (Utils.areEqualProcesses(this.self, this.trusted)) {
        this.TS += 1;
        this.bebBroadcastNewEpoch();
      }
    }
  }

  public void bebBroadcastNewEpoch() {
    // TODO this is sus
    this.pipelineExecutor.submitToPipeline(
        this.pipelineId, MessageComposer.createBebBroadcastNewEpoch(this.systemId, this.TS));
  }

  public void startEpoch(int newTS) {
    this.pipelineExecutor.submitToPipeline(
        this.pipelineId, MessageComposer.createStartEpoch(this.trusted, newTS));
  }

  private void sendNack(CommunicationProtocol.ProcessId destination, String toAbstractionId) {
    this.pipelineExecutor.submitToPipeline(
        this.pipelineId,
        MessageComposer.createSendNack(this.systemId, destination, toAbstractionId));
  }
}
