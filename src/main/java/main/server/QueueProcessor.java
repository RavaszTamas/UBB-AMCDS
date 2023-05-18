package main.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import main.CommunicationProtocol;
import main.algorithms.BestEffortBroadcast;
import main.pipelines.PipelineExecutor;
import main.utils.MessageComposer;
import main.utils.Utils;

public class QueueProcessor implements Runnable {

  private final String myIp;
  private final String owner;

  private final int listeningPort;
  private final int index;
  private final String hubIp;
  private final int hubPort;
  private final BlockingQueue<CommunicationProtocol.Message> messages;
  private final Map<String, List<CommunicationProtocol.ProcessId>> systems;
  private final Map<String, PipelineExecutor> pipelineExecutorMap;
  private CommunicationProtocol.ProcessId ownerProcess;

  public QueueProcessor(
      String hubIp, int hubPort, String myIp, int listeningPort, String owner, int index) {
    this.myIp = myIp;
    this.listeningPort = listeningPort;
    this.index = index;
    this.hubIp = hubIp;
    this.hubPort = hubPort;
    this.owner = owner;
    this.messages = new LinkedBlockingDeque<>();
    this.systems = new HashMap<>();
    this.pipelineExecutorMap = new HashMap<>();
    registerHandlers();
  }

  public Map<String, List<CommunicationProtocol.ProcessId>> getSystems() {
    return systems;
  }

  private void registerHandlers() {}

  public void addMessage(CommunicationProtocol.Message message) {
    this.messages.add(message);
  }

  private void handleProcessInitializeSystemMessageHandler(CommunicationProtocol.Message message) {
    System.out.println(
        "Received Communication System Initialization "
            + this.index
            + " for system "
            + message.getSystemId());
    List<CommunicationProtocol.ProcessId> processIds =
        message.getProcInitializeSystem().getProcessesList();
    processIds.forEach(
        processId -> {
          if (processId.getPort() == this.listeningPort) {
            this.ownerProcess = processId;
          }
        });
    this.systems.put(message.getSystemId(), processIds);
    this.pipelineExecutorMap.put(
        message.getSystemId(), new PipelineExecutor(this, message.getSystemId()));
  }

  private void handleProcessDestroySystemMessageHandler(CommunicationProtocol.Message message) {
    System.out.println(
        "Received Communication System Destruction "
            + this.index
            + " for system "
            + message.getSystemId());
    this.systems.remove(message.getSystemId());
    this.pipelineExecutorMap.remove(message.getSystemId());
  }

  private void handleAppValue(CommunicationProtocol.Message message) {
    System.out.printf(
        "%d Received value: %d\n", this.index, message.getAppValue().getValue().getV());
    String systemId = message.getSystemId();

    var executor = this.pipelineExecutorMap.get(systemId);

    executor.submitToPipeline(
        "app.pl",
        MessageComposer.createPlSend(
            message.getSystemId(),
            CommunicationProtocol.ProcessId.newBuilder()
                .setHost(this.hubIp)
                .setPort(this.hubPort)
                .build(),
            "app.pl",
            CommunicationProtocol.Message.newBuilder()
                .setType(CommunicationProtocol.Message.Type.APP_VALUE)
                .setAppValue(message.getAppValue())
                .build()));

    //    this.messages.add(
    //        MessageComposer.createPlSend(
    //            message.getSystemId(),
    //            CommunicationProtocol.ProcessId.newBuilder()
    //                .setHost(this.hubIp)
    //                .setPort(this.hubPort)
    //                .build(),
    //            "app.pl",
    //            CommunicationProtocol.Message.newBuilder()
    //                .setType(CommunicationProtocol.Message.Type.APP_VALUE)
    //                .setAppValue(message.getAppValue())
    //                .build()));
  }

  private void handleAppBroadcast(CommunicationProtocol.Message message) {
    System.out.println("App broadcast");
    CommunicationProtocol.AppBroadcast appBroadcast = message.getAppBroadcast();
    String messageSystemId = message.getSystemId();
    if (!this.systems.containsKey(messageSystemId)) {
      System.out.println("System is not detected inside the app!");
      return;
    }

    var executor = this.pipelineExecutorMap.get(messageSystemId);

    executor.submitToPipeline(
        "app.beb.pl",
        MessageComposer.createBebBroadcast(
            messageSystemId, MessageComposer.createAppValue(appBroadcast.getValue())));

    //    this.messages.add(
    //        MessageComposer.createBebBroadcast(
    //            messageSystemId, MessageComposer.createAppValue(appBroadcast.getValue())));
  }

  private void handleAppWrite(CommunicationProtocol.Message message) {
    String register = message.getAppWrite().getRegister();
    CommunicationProtocol.Value value = message.getAppWrite().getValue();
    String messageSystemId = message.getSystemId();
    var executor = this.pipelineExecutorMap.get(messageSystemId);
    String pipelineID = "app.nnar[" + register + "].beb.pl";

    System.out.printf(
        "%d Starts writing register %s value: %d\n", this.index, register, value.getV());
    executor.submitToPipeline(
        pipelineID, MessageComposer.createNNarWrite(register, value.getV(), messageSystemId));
  }

  private void handleAppRead(CommunicationProtocol.Message message) {
    String register = message.getAppRead().getRegister();
    String messageSystemId = message.getSystemId();
    var executor = this.pipelineExecutorMap.get(messageSystemId);
    String pipelineID = "app.nnar[" + register + "].beb.pl";

    System.out.printf("%d Starts reading register %s\n", this.index, register);
    executor.submitToPipeline(
        pipelineID, MessageComposer.createNNARRead(register, messageSystemId));
  }

  private void handleNNARReadReturn(CommunicationProtocol.Message message) {
    String systemId = message.getSystemId();
    boolean defined = message.getNnarReadReturn().getValue().getDefined();
    int readValue = message.getNnarReadReturn().getValue().getV();
    String register = Utils.getRegisterIdFromAbstraction(message.getFromAbstractionId());
    System.out.printf(
        "%d Read value: %d from register (isDefined:%s) %s\n",
        this.index, readValue, defined, register);
    var executor = this.pipelineExecutorMap.get(systemId);

    executor.submitToPipeline(
        "app.pl",
        MessageComposer.createAppReadReturnPlSend(
            systemId,
            CommunicationProtocol.ProcessId.newBuilder()
                .setHost(this.hubIp)
                .setPort(this.hubPort)
                .build(),
            register,
            defined ? readValue : null));
  }

  private void handleNNARWriteReturn(CommunicationProtocol.Message message) {
    String systemId = message.getSystemId();
    String register = Utils.getRegisterIdFromAbstraction(message.getFromAbstractionId());
    System.out.printf("%d Register %s writing done", this.index, register);
    var executor = this.pipelineExecutorMap.get(systemId);
    executor.submitToPipeline(
        "app.pl",
        MessageComposer.createAppWriteReturn(
            systemId,
            CommunicationProtocol.ProcessId.newBuilder()
                .setHost(this.hubIp)
                .setPort(this.hubPort)
                .build(),
            register));
  }

  private void handleAppPropose(CommunicationProtocol.Message message) {
//    System.out.println(message);
    String systemId = message.getSystemId();
    String topicName = message.getAppPropose().getTopic();
    CommunicationProtocol.Message ucMessage =
        CommunicationProtocol.Message.newBuilder()
            .setSystemId(systemId)
            .setToAbstractionId("app.uc[" + topicName + "].ec.pl")
            .setType(CommunicationProtocol.Message.Type.UC_PROPOSE)
            .setUcPropose(
                CommunicationProtocol.UcPropose.newBuilder()
                    .setValue(message.getAppPropose().getValue())
                    .build())
            .build();
    System.out.println(
        "App Received Rank: "
            + this.getOwnerProcess().getRank()
            + " Propose Value: "
            + message.getAppPropose().getValue().getV()
            + " in topic: "
            + message.getAppPropose().getTopic());
    var executor = this.pipelineExecutorMap.get(systemId);
    executor.submitToPipeline("app.uc[" + topicName + "].ec.pl", ucMessage);
  }

  private void handleUcDecide(CommunicationProtocol.Message message) {

    String systemID = message.getSystemId();

    // string register = m.AppWrite.Register;
    int value = message.getUcDecide().getValue().getV();

    System.out.println(this.index + " Decided: " + value);

    CommunicationProtocol.Message payloadMessage =
        CommunicationProtocol.Message.newBuilder()
            .setSystemId(systemID)
            .setType(CommunicationProtocol.Message.Type.PL_SEND)
            .setPlSend(
                CommunicationProtocol.PlSend.newBuilder()
                    .setDestination(
                        CommunicationProtocol.ProcessId.newBuilder()
                            .setHost(this.hubIp)
                            .setPort(this.hubPort)
                            .build())
                    .setMessage(
                        CommunicationProtocol.Message.newBuilder()
                            .setToAbstractionId("hub")
                            .setType(CommunicationProtocol.Message.Type.APP_DECIDE)
                            .setAppDecide(
                                CommunicationProtocol.AppDecide.newBuilder()
                                    .setValue(
                                        CommunicationProtocol.Value.newBuilder()
                                            .setDefined(message.getUcDecide().getValue().getDefined())
                                            .setV(value)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    var executor = this.pipelineExecutorMap.get(systemID);
    executor.submitToPipeline("app.pl", payloadMessage);
  }

  private void handleMessage(CommunicationProtocol.Message message) {
    //    System.out.println(topLevelMessage);
    //    System.out.println("Message from abstraction " + topLevelMessage.getFromAbstractionId());
    //    System.out.println("Message to abstraction " + topLevelMessage.getToAbstractionId());
    //    switch (message.getType()) {
    //      case PROC_INITIALIZE_SYSTEM -> {
    //        this.handleProcessInitializeSystemMessageHandler(message);
    //      }
    //      case PROC_DESTROY_SYSTEM -> {
    //        handleProcessDestroySystemMessageHandler(message);
    //      }
    //      case APP_VALUE -> {
    //        handleAppValue(message);
    //      }
    //      case NNAR_READ, NNAR_WRITE -> {
    //        atomicRegister.send(message);
    //      }
    //      case NNAR_INTERNAL_READ, NNAR_INTERNAL_WRITE, NNAR_INTERNAL_ACK, NNAR_INTERNAL_VALUE ->
    // {
    //        atomicRegister.deliver(message);
    //      }
    //      case NNAR_READ_RETURN -> {
    //        handleNNARReadReturn(message);
    //      }
    //      case NNAR_WRITE_RETURN -> {
    //        handleNNARWriteReturn(message);
    //      }
    //      case APP_READ -> {
    //        handleAppRead(message);
    //      }
    //      case APP_WRITE -> {
    //        handleAppWrite(message);
    //      }
    //      case APP_BROADCAST -> {
    //        handleAppBroadcast(message);
    //      }
    //      case BEB_BROADCAST -> {
    //        this.bestEffortBroadcast.send(message);
    //      }
    //      case BEB_DELIVER -> {
    //        this.bestEffortBroadcast.deliver(message);
    //      }
    //      case PL_SEND -> {
    //        this.perfectLink.send(message);
    //      }
    //      case PL_DELIVER -> {
    //        this.
    //      }
    //      default -> {
    //        System.out.println("UNKNOWN -- " + message.getType());
    //        System.out.println(message);
    //      }
    //    }

    switch (message.getType()) {
      case PROC_INITIALIZE_SYSTEM -> {
        this.handleProcessInitializeSystemMessageHandler(message);
      }
      case PROC_DESTROY_SYSTEM -> {
        handleProcessDestroySystemMessageHandler(message);
      }
      case APP_VALUE -> {
        this.handleAppValue(message);
      }
      case APP_BROADCAST -> {
        handleAppBroadcast(message);
      }
      case NNAR_READ_RETURN -> {
        handleNNARReadReturn(message);
      }
      case NNAR_WRITE_RETURN -> {
        handleNNARWriteReturn(message);
      }
      case PL_DELIVER -> {
        this.messages.add(
            CommunicationProtocol.Message.newBuilder(message.getPlDeliver().getMessage())
                .setPlDeliver(
                    CommunicationProtocol.PlDeliver.newBuilder()
                        .setSender(message.getPlDeliver().getSender())
                        .build())
                .setSystemId(message.getSystemId())
                .build());
      }
      case APP_WRITE -> {
        handleAppWrite(message);
      }
      case APP_READ -> {
        handleAppRead(message);
      }
      case APP_PROPOSE -> {
        handleAppPropose(message);
      }
      case UC_DECIDE -> {
        handleUcDecide(message);
      }
      default -> {
        System.out.println("UNKNOWN -- " + message.getType());
        System.out.println(message);
      }
    }
  }

  @Override
  public void run() {
    try {
      while (true) {
        CommunicationProtocol.Message message = messages.take();
        handleMessage(message);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public String getMyIp() {
    return myIp;
  }

  public String getOwner() {
    return owner;
  }

  public int getListeningPort() {
    return listeningPort;
  }

  public int getIndex() {
    return index;
  }

  public String getHubIp() {
    return hubIp;
  }

  public int getHubPort() {
    return hubPort;
  }

  public CommunicationProtocol.ProcessId getOwnerProcess() {
    return ownerProcess;
  }

  public void submitToSystemExecutorBottomUp(
      String systemId, String pipelineId, CommunicationProtocol.Message message) {
    //    System.out.println(pipelineId);
    //    System.out.println(message.getType());
    //    System.out.println(message.getPlDeliver().getMessage().getType());
    var executor = this.pipelineExecutorMap.get(systemId);
    executor.submitToPipeline(pipelineId, message);
  }
}
