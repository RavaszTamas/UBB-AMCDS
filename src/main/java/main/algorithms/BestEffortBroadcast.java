package main.algorithms;

import java.util.List;
import java.util.Map;
import main.CommunicationProtocol;
import main.server.QueueProcessor;
import main.utils.MessageComposer;

public class BestEffortBroadcast implements MessagingAlgorithm {

  private final QueueProcessor queueProcessor;

  public BestEffortBroadcast(QueueProcessor queueProcessor) {
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
            this.queueProcessor.addMessage(
                MessageComposer.createPlSend(
                    systemId,
                    destination,
                    message.getToAbstractionId(),
                    message.getBebBroadcast().getMessage())));
  }

  @Override
  public void deliver(CommunicationProtocol.Message message) {
    this.queueProcessor.addMessage(message.getBebDeliver().getMessage());
  }
}
