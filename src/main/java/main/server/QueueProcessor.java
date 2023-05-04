package main.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import main.CommunicationProtocol;
import main.algorithms.BestEffortBroadcast;
import main.algorithms.NNAtomicRegister;
import main.algorithms.PerfectLink;
import main.utils.MessageComposer;
import main.utils.Utils;

public class QueueProcessor implements Runnable {

  private final String myIp;
  private final String owner;

  private final int listeningPort;
  private final int index;
  private final String hubIp;
  private final int hubPort;
  private final PerfectLink perfectLink;
  private final BestEffortBroadcast bestEffortBroadcast;
  private final NNAtomicRegister atomicRegister;
  private final BlockingQueue<CommunicationProtocol.Message> messages;
  private final Map<String, List<CommunicationProtocol.ProcessId>> systems;

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
    registerHandlers();
    perfectLink = new PerfectLink(this);
    bestEffortBroadcast = new BestEffortBroadcast(this);
    atomicRegister = new NNAtomicRegister(this);
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
    this.systems.put(message.getSystemId(), message.getProcInitializeSystem().getProcessesList());
  }

  private void handleProcessDestroySystemMessageHandler(CommunicationProtocol.Message message) {
    System.out.println(
        "Received Communication System Destruction "
            + this.index
            + " for system "
            + message.getSystemId());
    this.systems.remove(message.getSystemId());
  }

  private void handleAppValue(CommunicationProtocol.Message message) {
    System.out.printf(
        "%d Received value: %d\n", this.index, message.getAppValue().getValue().getV());

    this.messages.add(
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
  }

  private void handleAppBroadcast(CommunicationProtocol.Message message) {
    System.out.println("App broadcast");
    System.out.println(message);
    CommunicationProtocol.AppBroadcast appBroadcast = message.getAppBroadcast();
    String messageSystemId = message.getSystemId();
    System.out.println(this.systems.keySet());
    if (!this.systems.containsKey(messageSystemId)) {
      System.out.println("System is not detected inside the app!");
      return;
    }

    this.messages.add(
        MessageComposer.createBebBroadcast(
            messageSystemId, MessageComposer.createAppValue(appBroadcast.getValue())));
  }

  private void handleAppWrite(CommunicationProtocol.Message message) {
    String register = message.getAppWrite().getRegister();
    CommunicationProtocol.Value value = message.getAppWrite().getValue();
    String messageSystemId = message.getSystemId();
    System.out.printf(
        "%d Starts writing register %s value: %d\n", this.index, register, value.getV());
    this.messages.add(MessageComposer.createNNarWrite(register, value.getV(), messageSystemId));
  }

  private void handleAppRead(CommunicationProtocol.Message message) {
    String register = message.getAppRead().getRegister();
    String messageSystemId = message.getSystemId();
    System.out.printf(
        "%d Starts reading register %s\n", this.index, register);
    this.messages.add(MessageComposer.createNNARRead(register, messageSystemId));
  }

  private void handleNNARReadReturn(CommunicationProtocol.Message message) {
    String systemId = message.getSystemId();
    boolean defined = message.getNnarReadReturn().getValue().getDefined();
    int readValue = message.getNnarReadReturn().getValue().getV();
    String register = Utils.getRegisterIdFromAbstraction(message.getFromAbstractionId());
    System.out.printf("%d Read value: %d from register (isDefined:%s) %s\n", this.index, readValue, defined, register);
    this.messages.add(
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
    this.messages.add(
        MessageComposer.createAppWriteReturn(
            systemId,
            CommunicationProtocol.ProcessId.newBuilder()
                .setHost(this.hubIp)
                .setPort(this.hubPort)
                .build(),
            register));
  }

  private void handleMessage(CommunicationProtocol.Message message) {
    //    System.out.println(topLevelMessage);
    //    System.out.println("Message from abstraction " + topLevelMessage.getFromAbstractionId());
    //    System.out.println("Message to abstraction " + topLevelMessage.getToAbstractionId());
    switch (message.getType()) {
      case PROC_INITIALIZE_SYSTEM -> {
        this.handleProcessInitializeSystemMessageHandler(message);
      }
      case PROC_DESTROY_SYSTEM -> {
        handleProcessDestroySystemMessageHandler(message);
      }
      case APP_VALUE -> {
        handleAppValue(message);
      }
      case NNAR_READ, NNAR_WRITE -> {
        atomicRegister.send(message);
      }
      case NNAR_INTERNAL_READ, NNAR_INTERNAL_WRITE, NNAR_INTERNAL_ACK, NNAR_INTERNAL_VALUE -> {
        atomicRegister.deliver(message);
      }
      case NNAR_READ_RETURN -> {
        handleNNARReadReturn(message);
      }
      case NNAR_WRITE_RETURN -> {
        handleNNARWriteReturn(message);
      }
      case APP_READ -> {
        handleAppRead(message);
      }
      case APP_WRITE -> {
        handleAppWrite(message);
      }
      case APP_BROADCAST -> {
        handleAppBroadcast(message);
      }
      case BEB_BROADCAST -> {
        this.bestEffortBroadcast.send(message);
      }
      case BEB_DELIVER -> {
        this.bestEffortBroadcast.deliver(message);
      }
      case PL_SEND -> {
        this.perfectLink.send(message);
      }
      case PL_DELIVER -> {
        this.perfectLink.deliver(message);
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
}
