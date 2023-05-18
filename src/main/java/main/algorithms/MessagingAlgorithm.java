package main.algorithms;

import main.CommunicationProtocol;

public interface MessagingAlgorithm {

  String getId();
  void send(CommunicationProtocol.Message message);

  void deliver(CommunicationProtocol.Message message);
}
