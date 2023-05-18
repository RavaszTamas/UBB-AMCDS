package main.pipelines;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import main.CommunicationProtocol;
import main.algorithms.MessagingAlgorithm;
import main.server.QueueProcessor;
import main.utils.Utils;

public class AbstractionPipeline {
  final List<MessagingAlgorithm> layers;
  final Map<String, MessagingAlgorithm> layerMap;
  final QueueProcessor queueProcessor;
  final BlockingQueue<CommunicationProtocol.Message> emittedMessages;
  final Thread executorThread;
  final String id;

  public AbstractionPipeline(
      String id, QueueProcessor queueProcessor, List<MessagingAlgorithm> layers) {
    this.id = id;
    this.layers = layers;
    this.queueProcessor = queueProcessor;
    this.emittedMessages = new LinkedBlockingDeque<>();
    this.layerMap = new HashMap<>();
    this.executorThread = new Thread(new PipelineThread());
    this.executorThread.start();
  }

  public void createLayerMap() {
    this.layers.forEach(
        layer -> {
          this.layerMap.put(layer.getId(), layer);
        });
  }

  public void addMessage(CommunicationProtocol.Message message) {
    this.emittedMessages.add(message);
  }

  private void handleMessage(CommunicationProtocol.Message message) {
    if(this.id.equals("app.pl")){
      System.out.println("---------App pl message---------");
      System.out.println(message);
    }
    if(message.getType().equals(CommunicationProtocol.Message.Type.EP_DECIDE)){
      System.out.println("DEcide mssage avsadasd\n" + message);
    }
    //        System.out.println("=========");
    //        System.out.println("Pipeline " + this.id + " got message " + message.getType());
    ////        System.out.println(message);
    //        System.out.println("=========");
    switch (message.getType()) {
      case PL_SEND -> {
        if (!this.layerMap.containsKey("pl")) return;
        var layer = this.layerMap.get("pl");
        layer.send(message);
      }
      case PL_DELIVER -> {
        if (!this.layerMap.containsKey("pl")) return;
        var layer = this.layerMap.get("pl");
        layer.deliver(message);
      }
      case BEB_BROADCAST -> {
        if (!this.layerMap.containsKey("beb")) return;
        var layer = this.layerMap.get("beb");
        layer.send(message);
      }
      case BEB_DELIVER -> {
        if (!this.layerMap.containsKey("beb")) return;
        var layer = this.layerMap.get("beb");
        layer.deliver(message);
      }
      case NNAR_READ, NNAR_WRITE -> {
        String registerId =
            "nnar[" + Utils.getRegisterIdFromAbstraction(message.getToAbstractionId()) + "]";
        if (!this.layerMap.containsKey(registerId)) return;
        var layer = this.layerMap.get(registerId);
        layer.send(message);
      }
      case NNAR_INTERNAL_READ, NNAR_INTERNAL_WRITE, NNAR_INTERNAL_ACK, NNAR_INTERNAL_VALUE -> {
        String registerId =
            "nnar[" + Utils.getRegisterIdFromAbstraction(message.getToAbstractionId()) + "]";
        if (!this.layerMap.containsKey(registerId)) return;
        var layer = this.layerMap.get(registerId);
        layer.deliver(message);
      }
      case EPFD_INTERNAL_HEARTBEAT_REQUEST, EPFD_INTERNAL_HEARTBEAT_REPLY -> {
        if (!this.layerMap.containsKey("epfd")) return;
        var layer = this.layerMap.get("epfd");
        layer.deliver(message);
      }
      case ELD_TRUST, EC_INTERNAL_NEW_EPOCH, EC_INTERNAL_NACK -> {
        if (!this.layerMap.containsKey("ec")) return;
        var layer = this.layerMap.get("ec");
        layer.deliver(message);
      }
      case EPFD_SUSPECT, EPFD_RESTORE -> {
        if (!this.layerMap.containsKey("eld")) return;
        var layer = this.layerMap.get("eld");
        layer.deliver(message);
      }
      case UC_PROPOSE -> {
        //        System.out.println(message);
        String consensusId =
            "uc[" + Utils.getRegisterIdFromAbstraction(message.getToAbstractionId()) + "]";
        if (!this.layerMap.containsKey(consensusId)) return;
        var layer = this.layerMap.get(consensusId);
        layer.send(message);
      }
      case EC_START_EPOCH, EP_ABORTED, EP_DECIDE -> {
        if (message.getType().equals(CommunicationProtocol.Message.Type.EP_DECIDE))
          System.out.println("Decided");
        System.out.println("before");
        System.out.println(message);
        System.out.println("after");
        String consensusId =
            "uc[" + Utils.getRegisterIdFromAbstraction(message.getToAbstractionId(), message) + "]";
        if (!this.layerMap.containsKey(consensusId)) return;
        var layer = this.layerMap.get(consensusId);
        layer.deliver(message);
      }
      case EP_PROPOSE, EP_ABORT -> {
        String epochConsensusId =
            "ep[" + Utils.getRegisterIdFromAbstraction(message.getToAbstractionId()) + "]";
        if (!this.layerMap.containsKey(epochConsensusId)) return;
        var layer = this.layerMap.get(epochConsensusId);
        layer.send(message);
      }
      case EP_INTERNAL_READ,
          EP_INTERNAL_WRITE,
          EP_INTERNAL_DECIDED,
          EP_INTERNAL_STATE,
          EP_INTERNAL_ACCEPT -> {
        String epochConsensusIdRegister =
            "ep[" + Utils.getRegisterIdFromAbstraction(message.getToAbstractionId()) + "]";
        String epochConsensusId =
            "ep[" + Utils.getEpConsensusId(message.getToAbstractionId()) + "]";

        if (message.getType().equals(CommunicationProtocol.Message.Type.EP_INTERNAL_DECIDED)) {
          System.out.println("Decided Internal");
          System.out.println(message.getToAbstractionId());
          System.out.println(epochConsensusId);
          System.out.println(epochConsensusIdRegister);
          System.out.println(this.layerMap.keySet());
          System.out.println(this.id);
        }
        String finalId;
        if (this.layerMap.containsKey(epochConsensusIdRegister)) {
          finalId = epochConsensusIdRegister;
        }
        else if (this.layerMap.containsKey(epochConsensusId)) {
          System.out.println("Somewhat different");
          System.out.println(message.getToAbstractionId());
          System.out.println(message.getType());
          finalId = epochConsensusId;
        }
        else return;
        var layer = this.layerMap.get(finalId);
        if (message.getType().equals(CommunicationProtocol.Message.Type.EP_INTERNAL_DECIDED)) {
          System.out.println("Delviering message");
        }

        layer.deliver(message);
      }
      default -> {
        System.out.println(
            this.id
                + "-- Layer for type "
                + message.getType()
                + " not found! Sending to app "
                + message.getToAbstractionId()
                + " !");
        this.queueProcessor.addMessage(message);
      }
    }
  }

  public void processBottomUp(CommunicationProtocol.Message message) {
    this.layers.get(this.layers.size() - 1).deliver(message);
  }

  public void processTopDown(CommunicationProtocol.Message message) {
    this.layers.get(0).send(message);
  }

  class PipelineThread implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          CommunicationProtocol.Message message = emittedMessages.take();
          handleMessage(message);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
