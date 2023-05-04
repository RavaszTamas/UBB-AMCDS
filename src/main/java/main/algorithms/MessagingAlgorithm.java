package main.algorithms;

import main.CommunicationProtocol;

public interface MessagingAlgorithm {

  void send(CommunicationProtocol.Message message);

  void deliver(CommunicationProtocol.Message message);
}
