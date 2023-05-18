package main.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import main.CommunicationProtocol;
import main.utils.MessageComposer;

public class WebsocketServer implements Runnable {

  private final String myIp;
  private final String owner;

  private final int listeningPort;
  private final int index;
  private final String hubIp;
  private final int hubPort;
  private final QueueProcessor queueProcessor;
  private ServerSocket serverSocket;

  public WebsocketServer(
      String hubIp, int hubPort, String myIp, int listeningPort, String owner, int index) {
    this.myIp = myIp;
    this.listeningPort = listeningPort;
    this.index = index;
    this.hubIp = hubIp;
    this.hubPort = hubPort;
    this.owner = owner;
    this.queueProcessor = new QueueProcessor(hubIp, hubPort, myIp, listeningPort, owner, index);
    Thread t = new Thread(this.queueProcessor);
    t.start();
  }

  @Override
  public void run() {
    System.out.println("Starting server " + this.listeningPort);
    try {
      this.serverSocket = new ServerSocket(listeningPort);
      while (true) {
        Socket socket = this.serverSocket.accept();
        InputStream stream = socket.getInputStream();
        byte[] bufferSize = stream.readNBytes(4);
        byte[] data = stream.readNBytes(ByteBuffer.wrap(bufferSize).getInt());
        socket.close();
        CommunicationProtocol.Message message =
            MessageComposer.convertNetworkMessageToPlDeliver(
                CommunicationProtocol.Message.parseFrom(data));
        if (message
                .getPlDeliver()
                .getMessage()
                .getType()
                .equals(CommunicationProtocol.Message.Type.PROC_INITIALIZE_SYSTEM)
            || message
                .getPlDeliver()
                .getMessage()
                .getType()
                .equals(CommunicationProtocol.Message.Type.PROC_DESTROY_SYSTEM))
          this.queueProcessor.addMessage(message);
        else
          this.queueProcessor.submitToSystemExecutorBottomUp(
              message.getSystemId(), message.getToAbstractionId(), message);
      }
    } catch (IOException ex) {
      System.out.println(ex.getMessage());
      ex.printStackTrace();
    }
  }
}
