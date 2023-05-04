package main.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Stream;

import main.CommunicationProtocol;
import main.utils.MessageComposer;

public class Process implements Runnable {

  private final String myIp;
  private final String owner;

  private final int listeningPort;
  private final int index;
  private final String hubIp;
  private final int hubPort;
  private final WebsocketServer server;

  public Process(
      String hubIp, int hubPort, String myIp, int listeningPort, String owner, int index) {
    this.myIp = myIp;
    this.owner = owner;
    this.listeningPort = listeningPort;
    this.index = index;
    this.hubIp = hubIp;
    this.hubPort = hubPort;
    server = new WebsocketServer(hubIp, hubPort, myIp, listeningPort, owner, index);
    Thread t = new Thread(server);
    t.start();
  }

  private void sendRegistrationMessage() throws IOException {
    CommunicationProtocol.Message message =
        MessageComposer.processRegistrationMessage(this.myIp, this.listeningPort, this.owner, this.index);

    Socket clientSocket = new Socket(this.hubIp, this.hubPort);
    OutputStream outputStream = clientSocket.getOutputStream();
    byte[] bytes = message.toByteArray();
    ByteBuffer byteBuffer = ByteBuffer.allocate(4 );
    byteBuffer.putInt(bytes.length);
    byte[] bufferSize = byteBuffer.array();

    byte[] messageBytes = new byte[bufferSize.length + bytes.length];
    System.arraycopy(bufferSize, 0, messageBytes, 0, bufferSize.length);
    System.arraycopy(bytes, 0, messageBytes, bufferSize.length, bytes.length);

    outputStream.write(messageBytes);
    outputStream.close();
  }

  @Override
  public void run() {
    try {
      this.sendRegistrationMessage();
      Thread.sleep(1000);
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }
}
