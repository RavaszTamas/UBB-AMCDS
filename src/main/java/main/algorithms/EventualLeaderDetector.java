package main.algorithms;

import main.CommunicationProtocol;
import main.pipelines.PipelineExecutor;
import main.server.QueueProcessor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class EventualLeaderDetector extends AbstractMessagingAlgorithm {
  QueueProcessor queueProcessor;
  String systemId;
  CommunicationProtocol.ProcessId leader;
  List<CommunicationProtocol.ProcessId> processes;
  List<CommunicationProtocol.ProcessId> suspected;

  public EventualLeaderDetector(
      String id,
      PipelineExecutor pipelineExecutor,
      QueueProcessor queueProcessor) {
    super(id, pipelineExecutor);
    this.systemId = this.pipelineExecutor.getSystemId();
    this.queueProcessor = queueProcessor;
    processes = this.queueProcessor.getSystems().get(this.systemId);
    leader =
        processes.stream()
            .max(Comparator.comparingInt(CommunicationProtocol.ProcessId::getRank))
            .orElseThrow();
    this.suspected = new ArrayList<>();
  }

  @Override
  public void send(CommunicationProtocol.Message message) {

  }

  @Override
  public void deliver(CommunicationProtocol.Message message) {
    if(message.getType().equals(CommunicationProtocol.Message.Type.EPFD_SUSPECT)){
      this.suspected.add(message.getEpfdSuspect().getProcess());
    }
    else if(message.getType().equals(CommunicationProtocol.Message.Type.EPFD_RESTORE)){
      this.suspected.remove(message.getEpfdSuspect().getProcess());
    }
  }
}
