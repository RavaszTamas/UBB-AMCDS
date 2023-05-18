package main.algorithms;

import main.CommunicationProtocol;
import main.pipelines.PipelineExecutor;
import main.server.QueueProcessor;
import main.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class EventuallyPerfectFailureDetector extends AbstractMessagingAlgorithm {

  List<CommunicationProtocol.ProcessId> processIdList;
  QueueProcessor queueProcessor;
  String systemId;
  String pipelineId;
  List<CommunicationProtocol.ProcessId> processes;
  List<CommunicationProtocol.ProcessId> alive;
  List<CommunicationProtocol.ProcessId> suspected;
  Timer timer;
  int deltaTime;
  int delay;

  public EventuallyPerfectFailureDetector(
      String id,
      PipelineExecutor pipelineExecutor,
      QueueProcessor queueProcessor,
      String pipelineId) {
    super(id, pipelineExecutor);
    this.systemId = this.pipelineExecutor.getSystemId();
    this.pipelineId = pipelineId;
    this.queueProcessor = queueProcessor;
    processes = this.queueProcessor.getSystems().get(this.systemId);
    alive = new ArrayList<>();
    suspected = new ArrayList<>();
    processes.forEach(
        process -> alive.add(CommunicationProtocol.ProcessId.newBuilder(process).build()));
    this.deltaTime = 100;
    this.delay = 100;
  }

  @Override
  public void send(CommunicationProtocol.Message message) {}

  @Override
  public void deliver(CommunicationProtocol.Message message) {
    if (message
        .getType()
        .equals(CommunicationProtocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)) {
      this.sendHeartBeatRequest(message.getPlDeliver().getSender());

    } else if (message
        .getType()
        .equals(CommunicationProtocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)) {
      synchronized (this) {
        if (Utils.CheckIfContainsProcessId(this.alive, message.getPlDeliver().getSender())) {
          this.alive.add(message.getPlDeliver().getSender());
        }
      }
    }
  }

  private void startTimer() {
    timer = new Timer();
    TimerTask task =
        new TimerTask() {
          public void run() {
            timeOut();
          }
        };

    timer.schedule(task, delay);
  }

  void timeOut() {
    synchronized (this) {
      if (Utils.isIntersection(this.alive, this.suspected)) this.delay += this.deltaTime;
      this.processes.forEach(
          processId -> {
            if (!this.alive.contains(processId) && !this.suspected.contains(processId)) {
              this.suspected.add(processId);
              this.deliverSuspected(processId);
            } else if (this.alive.contains(processId) && this.suspected.contains(processId)) {
              this.suspected.remove(processId);
              this.deliverRestored(processId);
            }
            this.sendHeartBeatRequest(processId);
          });
      this.alive.clear();
    }
    this.startTimer();
  }

  private void deliverSuspected(CommunicationProtocol.ProcessId processId) {
    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.EPFD_SUSPECT)
            .setEpfdSuspect(
                CommunicationProtocol.EpfdSuspect.newBuilder().setProcess(processId).build())
            .build();
    this.pipelineExecutor.submitToPipeline(this.pipelineId, message);
  }

  private void deliverRestored(CommunicationProtocol.ProcessId processId) {
    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.EPFD_RESTORE)
            .setEpfdRestore(
                CommunicationProtocol.EpfdRestore.newBuilder().setProcess(processId).build())
            .build();
    this.pipelineExecutor.submitToPipeline(this.pipelineId, message);
  }

  private void sendHeartBeatRequest(CommunicationProtocol.ProcessId processId) {
    CommunicationProtocol.Message message =
        CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.PL_SEND)
            .setToAbstractionId(this.id + ".pl")
            .setSystemId(this.systemId)
            .setPlSend(
                CommunicationProtocol.PlSend.newBuilder()
                    .setDestination(processId)
                    .setMessage(
                        CommunicationProtocol.Message.newBuilder()
                            .setType(
                                CommunicationProtocol.Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                            .setToAbstractionId(this.id)
                            .setEpfdInternalHeartbeatRequest(
                                CommunicationProtocol.EpfdInternalHeartbeatRequest.newBuilder()
                                    .build())
                            .build())
                    .build())
            .build();
    this.pipelineExecutor.submitToPipeline(this.pipelineId, message);
  }
}
