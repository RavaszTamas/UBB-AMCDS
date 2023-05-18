package main.algorithms;

import main.pipelines.PipelineExecutor;

public abstract class AbstractMessagingAlgorithm implements MessagingAlgorithm{
  protected final String id;
  protected final PipelineExecutor pipelineExecutor;

  public AbstractMessagingAlgorithm(String id, PipelineExecutor pipelineExecutor) {
    this.id = id;
    this.pipelineExecutor = pipelineExecutor;
  }

  public String getId(){
    return id;
  }
}
