package main.algorithms;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import main.CommunicationProtocol;
import main.pipelines.AbstractionPipeline;
import main.pipelines.PipelineExecutor;
import main.server.QueueProcessor;
import main.utils.MessageComposer;

public class PerfectLink extends AbstractMessagingAlgorithm {
  private final QueueProcessor queueProcessor;

  public PerfectLink(String id, PipelineExecutor pipelineExecutor, QueueProcessor queueProcessor) {
    super(id, pipelineExecutor);
    this.queueProcessor = queueProcessor;
  }

  @Override
  public void send(CommunicationProtocol.Message message) {
    try {
      CommunicationProtocol.Message networkMessage =
          MessageComposer.convertPlSendMessageToNetworkMessage(
              this.queueProcessor.getMyIp(), this.queueProcessor.getListeningPort(), message);
      //            System.out.println(
      //                this.queueProcessor.getIndex()
      //                    + " - Sending to "
      //                    + message.getPlSend().getDestination().getPort()
      //                    + " "
      //                    + message.getPlSend().getMessage().getType()
      //                    + " "
      //                    + message.getToAbstractionId());
      this.sendTcpMessage(
          networkMessage,
          message.getPlSend().getDestination().getHost(),
          message.getPlSend().getDestination().getPort());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void sendTcpMessage(
      CommunicationProtocol.Message message, String targetIp, int targetPort) throws IOException {
    Socket clientSocket = new Socket(targetIp, targetPort);
    OutputStream outputStream = clientSocket.getOutputStream();
    byte[] bytes = message.toByteArray();
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    byteBuffer.putInt(bytes.length);
    byte[] bufferSize = byteBuffer.array();

    byte[] messageBytes = new byte[bufferSize.length + bytes.length];
    System.arraycopy(bufferSize, 0, messageBytes, 0, bufferSize.length);
    System.arraycopy(bytes, 0, messageBytes, bufferSize.length, bytes.length);

    outputStream.write(messageBytes);
    outputStream.close();
  }

  @Override
  public void deliver(CommunicationProtocol.Message message) {

    CommunicationProtocol.Message contentMessage = message.getPlDeliver().getMessage();
    this.pipelineExecutor.submitToPipeline(
        message.getToAbstractionId(),
        CommunicationProtocol.Message.newBuilder(contentMessage)
            .setPlDeliver(
                CommunicationProtocol.PlDeliver.newBuilder()
                    .setSender(message.getPlDeliver().getSender())
                    .build())
            .setSystemId(message.getSystemId())
            .setToAbstractionId(message.getToAbstractionId())
            .build());
  }
}
