package main.algorithms;

import java.util.List;
import java.util.Map;
import main.CommunicationProtocol;
import main.pipelines.AbstractionPipeline;
import main.pipelines.PipelineExecutor;
import main.server.QueueProcessor;
import main.utils.MessageComposer;

public class BestEffortBroadcast extends AbstractMessagingAlgorithm {

  private final QueueProcessor queueProcessor;

  public BestEffortBroadcast(
      String id, PipelineExecutor pipelineExecutor, QueueProcessor queueProcessor) {
    super(id, pipelineExecutor);
    this.queueProcessor = queueProcessor;
  }

  @Override
  public void send(CommunicationProtocol.Message message) {
    Map<String, List<CommunicationProtocol.ProcessId>> systems = this.queueProcessor.getSystems();
    String systemId = message.getSystemId();
    if (!systems.containsKey(systemId)) {
      System.out.println("System not found!");
      return;
    }
    List<CommunicationProtocol.ProcessId> systemMembers = systems.get(systemId);
    systemMembers.forEach(
        destination ->
            this.pipelineExecutor.submitToPipeline(
                message.getToAbstractionId(),
                MessageComposer.createPlSend(
                    systemId,
                    destination,
                    message.getToAbstractionId(),
                    message.getBebBroadcast().getMessage())));
  }

  @Override
  public void deliver(CommunicationProtocol.Message message) {
    this.pipelineExecutor.submitToPipeline(
        message.getToAbstractionId(), message.getBebDeliver().getMessage());
  }
}
