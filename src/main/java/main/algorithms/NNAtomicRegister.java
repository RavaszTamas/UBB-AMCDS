package main.algorithms;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import main.CommunicationProtocol;
import main.domain.NNARStruct;
import main.domain.Register;
import main.server.QueueProcessor;
import main.utils.MessageComposer;
import main.utils.Utils;

public class NNAtomicRegister implements MessagingAlgorithm {

  private final QueueProcessor queueProcessor;
  private final Map<String, Register> registerMap;

  public NNAtomicRegister(QueueProcessor queueProcessor) {
    this.queueProcessor = queueProcessor;
    this.registerMap = new HashMap<>();
  }

  @Override
  public void send(CommunicationProtocol.Message message) {

    if (message.getType().equals(CommunicationProtocol.Message.Type.NNAR_READ)) {
      this.initializeRegister(message.getAppWrite().getRegister(), message.getSystemId());
      this.read(message);

    } else if (message.getType().equals(CommunicationProtocol.Message.Type.NNAR_WRITE)) {
      this.initializeRegister(message.getAppWrite().getRegister(), message.getSystemId());
      this.write(message);
    }
  }

  @Override
  public void deliver(CommunicationProtocol.Message message) {
    String registerId = Utils.getRegisterIdFromAbstraction(message.getToAbstractionId());
    System.out.println(
        this.queueProcessor.getIndex()
            + " - "
            + message.getType()
            + " "
            + message.getPlDeliver().getSender().getPort()
            + " "
            + message.getToAbstractionId());
    if (message.getType().equals(CommunicationProtocol.Message.Type.NNAR_INTERNAL_READ)) {
      this.initializeRegister(registerId, message.getSystemId());
      this.sendValue(message);
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.NNAR_INTERNAL_WRITE)) {
      this.initializeRegister(registerId, message.getSystemId());
      this.deliverWrite(message);
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.NNAR_INTERNAL_VALUE)) {
      this.initializeRegister(registerId, message.getSystemId());
      this.deliverValue(message);
    } else if (message.getType().equals(CommunicationProtocol.Message.Type.NNAR_INTERNAL_ACK)) {
      this.initializeRegister(registerId, message.getSystemId());
      this.deliverAck(message);
    }
  }

  private void sendValue(CommunicationProtocol.Message message) {
    String registerId = Utils.getRegisterIdFromAbstraction(message.getToAbstractionId());
    CommunicationProtocol.ProcessId sender = message.getPlDeliver().getSender();
    Register register = this.registerMap.get(registerId);
    this.queueProcessor.addMessage(
        MessageComposer.createNNARInternalValuePlSendPayload(
            registerId,
            sender,
            message.getSystemId(),
            message.getNnarInternalRead().getReadId(),
            register.getValueStruct()));
  }

  private void deliverValue(CommunicationProtocol.Message message) {
    int rid = message.getNnarInternalValue().getReadId();
    int ts = message.getNnarInternalValue().getTimestamp();
    int wr = message.getNnarInternalValue().getWriterRank();
    Integer val =
        message.getNnarInternalValue().getValue().getDefined()
            ? message.getNnarInternalValue().getValue().getV()
            : null;
    CommunicationProtocol.ProcessId sender = message.getPlDeliver().getSender();

    String registerId = Utils.getRegisterIdFromAbstraction(message.getToAbstractionId());
    String systemId = message.getSystemId();
    Register register = this.registerMap.get(registerId);
    if (register.getRid() == rid) {
      int senderIndexByPort = Utils.getIndexByPort(register.getProcessIds(), sender.getPort());
      NNARStruct[] readList = register.getReadList();
      readList[senderIndexByPort] = new NNARStruct(ts, wr, val);
      long nonNullCount = Arrays.stream(readList).filter(Objects::nonNull).count();
      System.out.println(Arrays.toString(readList));
      System.out.println(nonNullCount);
      if (nonNullCount > readList.length / 2) {
        NNARStruct max = Utils.getValueStructWithHighestTimestamp(readList);
        register.setReadVal(max.getVal());
        Arrays.fill(readList, null);

        if (register.isReading()) {
          this.bebBroadcastWrite(systemId, registerId, rid, max.getTs(), max.getWr(), max.getVal());
        } else {
          this.bebBroadcastWrite(
              systemId,
              registerId,
              rid,
              max.getTs() + 1,
              register.getSelfRank(),
              register.getWriteVal());
        }
      }
    }
  }

  private void deliverWrite(CommunicationProtocol.Message message) {
    String systemId = message.getSystemId();
    String registerId = Utils.getRegisterIdFromAbstraction(message.getToAbstractionId());
    int rid = message.getNnarInternalWrite().getReadId();
    int ts = message.getNnarInternalWrite().getTimestamp();
    int wr = message.getNnarInternalWrite().getWriterRank();
    int val = message.getNnarInternalWrite().getValue().getV();
    Register register = this.registerMap.get(registerId);
    if (ts > register.getValueStruct().getTs()
        || (ts == register.getValueStruct().getTs() && wr > register.getValueStruct().getWr()))
      register.setValueStruct(new NNARStruct(ts, wr, val));
    CommunicationProtocol.ProcessId sender = message.getPlDeliver().getSender();

    this.queueProcessor.addMessage(
        MessageComposer.createPlSend(
            systemId,
            sender,
            "app.nnar[" + registerId + "].pl",
            MessageComposer.createNNARInternalAck("app.nnar[" + registerId + "]", rid)));
  }

  private void deliverAck(CommunicationProtocol.Message message) {
    String systemId = message.getSystemId();
    String registerId = Utils.getRegisterIdFromAbstraction(message.getToAbstractionId());
    Register register = this.registerMap.get(registerId);
    if (register.getRid() == message.getNnarInternalAck().getReadId()) {
      register.setAcks(register.getAcks() + 1);
      if (register.getAcks() > register.getReadList().length / 2) {
        register.setAcks(0);
        CommunicationProtocol.Message newPayload;
        if (register.isReading()) {
          register.setReading(false);
          newPayload =
              MessageComposer.createNNARReadReturn(systemId, registerId, register.getReadVal());
        } else {
          newPayload = MessageComposer.createNNARWriteReturn(systemId, registerId);
        }
        this.queueProcessor.addMessage(newPayload);
      }
    }
  }

  private void initializeRegister(String registerId, String systemId) {
    if (!this.registerMap.containsKey(registerId)) {
      var processes = this.queueProcessor.getSystems().get(systemId);
      this.registerMap.put(
          registerId,
          new Register(
              0,
              new NNARStruct(),
              0,
              0,
              0,
              0,
              false,
              systemId,
              processes,
              new NNARStruct[processes.size()]));
    }
  }

  private void read(CommunicationProtocol.Message message) {
    String systemId = message.getSystemId();
    String registerId = Utils.getRegisterIdFromAbstraction(message.getToAbstractionId());
    Register register = this.registerMap.get(registerId);
    register.setAcks(0);
    register.setRid(register.getRid() + 1);
    register.setReading(true);
    NNARStruct[] readList = register.getReadList();
    Arrays.fill(readList, null);
    this.bebBroadcastInternalRead(systemId, registerId, register.getRid());
  }

  private void write(CommunicationProtocol.Message message) {
    String registerId = message.getAppWrite().getRegister();
    String systemId = message.getSystemId();
    int writeVal = message.getNnarWrite().getValue().getV();
    Register register = this.registerMap.get(registerId);
    register.setAcks(0);
    register.setWriteVal(writeVal);
    register.setRid(register.getRid() + 1);
    NNARStruct[] readList = register.getReadList();
    Arrays.fill(readList, null);
    this.bebBroadcastInternalRead(systemId, registerId, register.getRid());
  }

  private void bebBroadcastInternalRead(String systemId, String registerId, int readId) {
    CommunicationProtocol.Message message =
        MessageComposer.createBebBroadcast(
            systemId,
            "app.nnar[" + registerId + "].beb.pl",
            MessageComposer.createNNARInternalRead(systemId,"app.nnar[" + registerId + "]", readId));
    System.out.println(message);
    this.queueProcessor.addMessage(message);
  }

  private void bebBroadcastWrite(
      String systemId, String registerId, int readId, int timestamp, int rank, Integer value) {

    CommunicationProtocol.Message message =
        MessageComposer.createBebBroadcast(
            systemId,
            "app.nnar[" + registerId + "].beb.pl",
            MessageComposer.createNNARInternalWrite(
                "app.nnar[" + registerId + "]", readId, timestamp, rank, value));

    this.queueProcessor.addMessage(message);
  }
}
