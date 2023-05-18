package main.pipelines;

import java.util.*;

import main.CommunicationProtocol;
import main.algorithms.*;
import main.domain.EpochStateStruct;
import main.server.QueueProcessor;
import main.utils.Utils;

public class PipelineExecutor {
  final String systemId;
  final QueueProcessor queueProcessor;
  final Map<String, MessagingAlgorithm> layerCache;
  final Map<String, MessagingAlgorithm> uniqueLayerCache;
  final Map<String, AbstractionPipeline> abstractionPipelineMap;

  public PipelineExecutor(QueueProcessor queueProcessor, String systemId) {
    this.queueProcessor = queueProcessor;
    this.layerCache = new HashMap<>();
    this.uniqueLayerCache = new HashMap<>();
    this.abstractionPipelineMap = new HashMap<>();
    this.systemId = systemId;
  }

  public synchronized void generatePipeline(String id) {
    System.out.println("Generating pipeline " + id);
    if (this.abstractionPipelineMap.containsKey(id)) return;
    this.abstractionPipelineMap.put(id,null);
    var layerIds = id.split("\\.");
    List<MessagingAlgorithm> messagingAlgorithmList = new ArrayList<>();
    AbstractionPipeline abstractionPipeline =
        new AbstractionPipeline(id, queueProcessor, messagingAlgorithmList);
    for (int i = 1; i < layerIds.length; i++) {
      //              System.out.println(layerId);
      //              System.out.println(layerCache);
      if (layerCache.containsKey(layerIds[i])) {

        messagingAlgorithmList.add(this.layerCache.get(layerIds[i]));
      } else if (uniqueLayerCache.containsKey(layerIds[i - 1] + "." + layerIds[i])) {
        messagingAlgorithmList.add(this.uniqueLayerCache.get(layerIds[i - 1] + "." + layerIds[i]));
      } else {
        var algorithm = this.createLayer(layerIds[i], id);
        this.layerCache.put(layerIds[i], algorithm);
        messagingAlgorithmList.add(algorithm);
      }
    }

    abstractionPipeline.createLayerMap();
    this.abstractionPipelineMap.put(id, abstractionPipeline);

    abstractionPipeline.layers.forEach(
        item -> {
          if (item instanceof UniformConsensus) {
            ((UniformConsensus) item).init();
          }
        });
  }

  private MessagingAlgorithm createLayer(String id, String pipelineId) {
    if (id.contains("pl")) {
      return new PerfectLink(id, this, this.queueProcessor);
    } else if (id.contains("beb")) {
      return new BestEffortBroadcast(id, this, this.queueProcessor);
    } else if (id.contains("nnar")) {
      if (this.layerCache.containsKey(id)) throw new IllegalArgumentException("Already exists!");
      return new NNAtomicRegister(id, this, this.queueProcessor);
    } else if (id.contains("ec")) {
      return new EpochChange(id, this, queueProcessor, pipelineId);
    } else if (id.contains("ep")) {
      String epochConsensusIdRegister =
              "uc[" + Utils.getRegisterIdFromAbstraction(pipelineId) + "]";
      return new EpochConsensus(
          id,
          this,
          new EpochStateStruct(
              CommunicationProtocol.Value.newBuilder().setDefined(false).setV(0).build(), 0),
          queueProcessor,
          this.queueProcessor.getSystems().get(this.systemId).size(),
          0,
              epochConsensusIdRegister);
    } else if (id.contains("eld")) {
      return new EventualLeaderDetector(id, this, queueProcessor);
    } else if (id.contains("epfd")) {
      return new EventuallyPerfectFailureDetector(id, this, queueProcessor, pipelineId);
    } else if (id.contains("uc")) {
      return new UniformConsensus(
          id,
          this,
          queueProcessor,
          pipelineId,
          this.queueProcessor.getSystems().get(this.systemId).size());
    } else {
      throw new IllegalArgumentException("can't create a pipeline with id" + id);
    }
  }

  public void processBottomUp(String pipelineId, CommunicationProtocol.Message message) {
    if (!this.abstractionPipelineMap.containsKey(pipelineId)) {
      System.out.println("Abstraction not found: \"" + pipelineId + "\"");
      this.generatePipeline(pipelineId);
    }
    if (this.abstractionPipelineMap.containsKey(pipelineId)) {
      this.abstractionPipelineMap.get(pipelineId).processBottomUp(message);
    }
  }

  public void processTopDown(String pipelineId, CommunicationProtocol.Message message) {
    if (!this.abstractionPipelineMap.containsKey(pipelineId)) {
      System.out.println("Abstraction not found: \"" + pipelineId + "\"");
      this.generatePipeline(pipelineId);
    }
    if (this.abstractionPipelineMap.containsKey(pipelineId)) {
      this.abstractionPipelineMap.get(pipelineId).processTopDown(message);
    }
  }

  public synchronized void submitToPipeline(
      String pipelineId, CommunicationProtocol.Message message) {
    if (!this.abstractionPipelineMap.containsKey(pipelineId)) {
      System.out.println("Abstraction not found: \"" + pipelineId + "\"");
      this.generatePipeline(pipelineId);
    }
    if (message.getType().equals(CommunicationProtocol.Message.Type.UC_DECIDE)) {
      System.out.println(message);
      System.out.println(this.abstractionPipelineMap.containsKey(pipelineId));
    }
    if (this.abstractionPipelineMap.containsKey(pipelineId)) {
      this.abstractionPipelineMap.get(pipelineId).addMessage(message);
    }
  }

  public String getSystemId() {
    return systemId;
  }

  public QueueProcessor getQueueProcessor() {
    return queueProcessor;
  }

  public Map<String, MessagingAlgorithm> getLayerCache() {
    return layerCache;
  }

  public Map<String, MessagingAlgorithm> getUniqueLayerCache() {
    return uniqueLayerCache;
  }

  public Map<String, AbstractionPipeline> getAbstractionPipelineMap() {
    return abstractionPipelineMap;
  }
}
