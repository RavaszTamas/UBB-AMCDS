package main.utils;

import main.CommunicationProtocol;
import main.domain.NNARStruct;

public final class MessageComposer {
  public static CommunicationProtocol.Message createAppValue(CommunicationProtocol.Value value) {
    return CommunicationProtocol.Message.newBuilder()
        .setType(CommunicationProtocol.Message.Type.APP_VALUE)
        .setFromAbstractionId("app")
        .setToAbstractionId("app")
        .setAppValue(CommunicationProtocol.AppValue.newBuilder().setValue(value).build())
        .build();
  }

  public static CommunicationProtocol.Message createPlSend(
      String systemId,
      CommunicationProtocol.ProcessId destination,
      String abstractionId,
      CommunicationProtocol.Message messagePayload) {
    return CommunicationProtocol.Message.newBuilder()
        .setSystemId(systemId)
        .setToAbstractionId(abstractionId)
        .setType(CommunicationProtocol.Message.Type.PL_SEND)
        .setPlSend(
            CommunicationProtocol.PlSend.newBuilder()
                .setDestination(destination)
                .setMessage(messagePayload)
                .build())
        .build();
  }

  public static CommunicationProtocol.Message createBebBroadcast(
      String systemId, CommunicationProtocol.Message messageToSend) {
    return CommunicationProtocol.Message.newBuilder()
        .setType(CommunicationProtocol.Message.Type.BEB_BROADCAST)
        .setSystemId(systemId)
        .setToAbstractionId("app.beb.pl")
        .setBebBroadcast(
            CommunicationProtocol.BebBroadcast.newBuilder().setMessage(messageToSend).build())
        .build();
  }

  public static CommunicationProtocol.Message createBebBroadcast(
      String systemId, String toAbstraction, CommunicationProtocol.Message messageToSend) {
    return CommunicationProtocol.Message.newBuilder()
        .setType(CommunicationProtocol.Message.Type.BEB_BROADCAST)
        .setSystemId(systemId)
        .setToAbstractionId(toAbstraction)
        .setBebBroadcast(
            CommunicationProtocol.BebBroadcast.newBuilder().setMessage(messageToSend).build())
        .build();
  }

  public static CommunicationProtocol.Message createNNARInternalRead(
      String systemId, String toAbstractionId, int readId) {
    return CommunicationProtocol.Message.newBuilder()
        .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_READ)
        .setFromAbstractionId(toAbstractionId)
        .setToAbstractionId(toAbstractionId)
        .setSystemId(systemId)
        .setNnarInternalRead(
            CommunicationProtocol.NnarInternalRead.newBuilder().setReadId(readId).build())
        .build();
  }

  public static CommunicationProtocol.Message createNNARInternalWrite(
      String toAbstractionId, int readId, int timestamp, int rank, Integer value) {
    return CommunicationProtocol.Message.newBuilder()
        .setFromAbstractionId(toAbstractionId)
        .setToAbstractionId(toAbstractionId)
        .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_WRITE)
        .setNnarInternalWrite(
            CommunicationProtocol.NnarInternalWrite.newBuilder()
                .setReadId(readId)
                .setTimestamp(timestamp)
                .setWriterRank(rank)
                .setValue(
                    value == null
                        ? CommunicationProtocol.Value.newBuilder().setDefined(false).build()
                        : CommunicationProtocol.Value.newBuilder()
                            .setV(value)
                            .setDefined(true)
                            .build())
                .build())
        .build();
  }

  public static CommunicationProtocol.Message createNNARInternalAck(
      String abstraction, int readId) {
    return CommunicationProtocol.Message.newBuilder()
        .setToAbstractionId(abstraction)
        .setFromAbstractionId(abstraction)
        .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_ACK)
        .setNnarInternalAck(
            CommunicationProtocol.NnarInternalAck.newBuilder().setReadId(readId).build())
        .build();
  }

  public static CommunicationProtocol.Message createNNARInternalValuePlSendPayload(
      String registerId,
      CommunicationProtocol.ProcessId destination,
      String systemId,
      int readId,
      NNARStruct NNARStruct) {
    return CommunicationProtocol.Message.newBuilder()
        .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_VALUE)
        .setFromAbstractionId("app.nnar[" + registerId + "]")
        .setToAbstractionId("app.nnar[" + registerId + "].pl")
        .setSystemId(systemId)
        .setType(CommunicationProtocol.Message.Type.PL_SEND)
        .setPlSend(
            CommunicationProtocol.PlSend.newBuilder()
                .setDestination(destination)
                .setMessage(
                    CommunicationProtocol.Message.newBuilder()
                        .setType(CommunicationProtocol.Message.Type.NNAR_INTERNAL_VALUE)
                        .setFromAbstractionId("app.nnar[" + registerId + "]")
                        .setToAbstractionId("app.nnar[" + registerId + "]")
                        .setNnarInternalValue(
                            CommunicationProtocol.NnarInternalValue.newBuilder()
                                .setReadId(readId)
                                .setTimestamp(NNARStruct.getTs())
                                .setWriterRank(NNARStruct.getWr())
                                .setValue(
                                    NNARStruct.getVal() == null
                                        ? CommunicationProtocol.Value.newBuilder()
                                            .setDefined(false)
                                            .build()
                                        : CommunicationProtocol.Value.newBuilder()
                                            .setDefined(true)
                                            .setV(NNARStruct.getVal())
                                            .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  public static CommunicationProtocol.Message createNNARReadReturn(
      String systemId, String registerId, Integer value) {
    return CommunicationProtocol.Message.newBuilder()
        .setSystemId(systemId)
        .setFromAbstractionId("app.nnar[" + registerId + "]")
        .setToAbstractionId("app")
        .setType(CommunicationProtocol.Message.Type.NNAR_READ_RETURN)
        .setNnarReadReturn(
            CommunicationProtocol.NnarReadReturn.newBuilder()
                .setValue(
                    value == null
                        ? CommunicationProtocol.Value.newBuilder().setDefined(false).build()
                        : CommunicationProtocol.Value.newBuilder()
                            .setDefined(true)
                            .setV(value)
                            .build())
                .build())
        .build();
  }

  public static CommunicationProtocol.Message createNNARWriteReturn(
      String systemId, String registerId) {
    return CommunicationProtocol.Message.newBuilder()
        .setSystemId(systemId)
        .setFromAbstractionId("app.nnar[" + registerId + "]")
        .setToAbstractionId("app")
        .setType(CommunicationProtocol.Message.Type.NNAR_WRITE_RETURN)
        .setNnarWriteReturn(CommunicationProtocol.NnarWriteReturn.newBuilder().build())
        .build();
  }

  public static CommunicationProtocol.Message createAppReadReturnPlSend(
      String systemId,
      CommunicationProtocol.ProcessId destination,
      String register,
      Integer value) {
    return CommunicationProtocol.Message.newBuilder()
        .setSystemId(systemId)
        .setType(CommunicationProtocol.Message.Type.PL_SEND)
        //        .setToAbstractionId("hub")
        .setFromAbstractionId("app")
        .setPlSend(
            CommunicationProtocol.PlSend.newBuilder()
                .setDestination(destination)
                .setMessage(
                    CommunicationProtocol.Message.newBuilder()
                        .setFromAbstractionId("app")
                        .setToAbstractionId("hub")
                        .setType(CommunicationProtocol.Message.Type.APP_READ_RETURN)
                        .setAppReadReturn(
                            CommunicationProtocol.AppReadReturn.newBuilder()
                                .setRegister(register)
                                .setValue(
                                    value == null
                                        ? CommunicationProtocol.Value.newBuilder()
                                            .setDefined(false)
                                            .build()
                                        : CommunicationProtocol.Value.newBuilder()
                                            .setDefined(true)
                                            .setV(value)
                                            .build())
                                .build())
                        .build())
                .build())
        .build();
  }

  public static CommunicationProtocol.Message createAppWriteReturn(
      String systemId, CommunicationProtocol.ProcessId destination, String register) {
    return CommunicationProtocol.Message.newBuilder()
        .setType(CommunicationProtocol.Message.Type.PL_SEND)
        .setSystemId(systemId)
        .setFromAbstractionId("app")
        .setPlSend(
            CommunicationProtocol.PlSend.newBuilder()
                .setDestination(destination)
                .setMessage(
                    CommunicationProtocol.Message.newBuilder()
                        .setFromAbstractionId("app")
                        .setToAbstractionId("hub")
                        .setType(CommunicationProtocol.Message.Type.APP_WRITE_RETURN)
                        .setAppWriteReturn(
                            CommunicationProtocol.AppWriteReturn.newBuilder()
                                .setRegister(register)
                                .build())
                        .build())
                .build())
        .build();
  }

  public static CommunicationProtocol.Message convertNetworkMessageToPlDeliver(
      CommunicationProtocol.Message message) {
    //    System.out.println("--------------");
    //    System.out.println(message);
    //    System.out.println(
    return CommunicationProtocol.Message.newBuilder()
        .setToAbstractionId(message.getToAbstractionId())
        .setSystemId(message.getSystemId())
        .setType(CommunicationProtocol.Message.Type.PL_DELIVER)
        .setPlDeliver(
            CommunicationProtocol.PlDeliver.newBuilder()
                .setSender(
                    CommunicationProtocol.ProcessId.newBuilder()
                        .setHost(message.getNetworkMessage().getSenderHost())
                        .setPort(message.getNetworkMessage().getSenderListeningPort())
                        .build())
                .setMessage(message.getNetworkMessage().getMessage())
                .build())
        .build();
    //    );
    //    System.out.println("--------------");
  }

  public static CommunicationProtocol.Message convertPlSendMessageToNetworkMessage(
      String myIp, int myPort, CommunicationProtocol.Message message) {
    //    System.out.println("--------------");
    //    System.out.println(message);
    //    System.out.println(
    //    System.out.println(message);
    return CommunicationProtocol.Message.newBuilder()
        .setToAbstractionId(message.getToAbstractionId())
        .setSystemId(message.getSystemId())
        .setType(CommunicationProtocol.Message.Type.NETWORK_MESSAGE)
        .setNetworkMessage(
            CommunicationProtocol.NetworkMessage.newBuilder()
                .setSenderListeningPort(myPort)
                .setMessage(message.getPlSend().getMessage())
                .build())
        .build();
    //    );
    //    System.out.println("--------------");
  }

  public static CommunicationProtocol.Message processRegistrationMessage(
      String myIp, int myPort, String owner, int index) {

    return CommunicationProtocol.Message.newBuilder()
        .setFromAbstractionId("app.pl")
        .setType(CommunicationProtocol.Message.Type.NETWORK_MESSAGE)
        .setNetworkMessage(
            CommunicationProtocol.NetworkMessage.newBuilder()
                .setSenderHost(myIp)
                .setSenderListeningPort(myPort)
                .setMessage(
                    CommunicationProtocol.Message.newBuilder()
                        .setType(CommunicationProtocol.Message.Type.PROC_REGISTRATION)
                        .setProcRegistration(
                            CommunicationProtocol.ProcRegistration.newBuilder()
                                .setOwner(owner)
                                .setIndex(index)
                                .build())
                        .build()))
        .build();
  }

  public static CommunicationProtocol.Message createNNarWrite(
      String registerId, int value, String systemId) {
    return CommunicationProtocol.Message.newBuilder()
        .setToAbstractionId("app.nar[" + registerId + "].beb.pl")
        .setType(CommunicationProtocol.Message.Type.NNAR_WRITE)
        .setSystemId(systemId)
        .setNnarWrite(
            CommunicationProtocol.NnarWrite.newBuilder()
                .setValue(
                    CommunicationProtocol.Value.newBuilder().setDefined(true).setV(value).build())
                .build())
        .setAppWrite(CommunicationProtocol.AppWrite.newBuilder().setRegister(registerId).build())
        .build();
  }

  public static CommunicationProtocol.Message createNNARRead(String registerId, String systemId) {
    return CommunicationProtocol.Message.newBuilder()
        .setToAbstractionId("app.nar[" + registerId + "].beb.pl")
        .setType(CommunicationProtocol.Message.Type.NNAR_READ)
        .setSystemId(systemId)
        .setNnarRead(CommunicationProtocol.NnarRead.newBuilder().build())
        .setAppWrite(CommunicationProtocol.AppWrite.newBuilder().setRegister(registerId).build())
        .build();
  }

  public static CommunicationProtocol.Message createBebBroadcastNewEpoch(String systemId, int TS) {
    return CommunicationProtocol.Message.newBuilder()
        .setType(CommunicationProtocol.Message.Type.BEB_BROADCAST)
        .setToAbstractionId("app.ec.beb.pl")
        .setSystemId(systemId)
        .setBebBroadcast(
            CommunicationProtocol.BebBroadcast.newBuilder()
                .setMessage(
                    CommunicationProtocol.Message.newBuilder()
                        .setToAbstractionId("app.ec")
                        .setType(CommunicationProtocol.Message.Type.EC_INTERNAL_NEW_EPOCH)
                        .setEcInternalNewEpoch(
                            CommunicationProtocol.EcInternalNewEpoch.newBuilder()
                                    .setTimestamp(TS)
                                    .build())
                        .build())
                .build())
        .build();
  }

  public static CommunicationProtocol.Message createStartEpoch(CommunicationProtocol.ProcessId trusted, int newTs){
    return CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.EC_START_EPOCH)
            .setEcStartEpoch(CommunicationProtocol.EcStartEpoch.newBuilder()
                    .setNewLeader(CommunicationProtocol.ProcessId.newBuilder(trusted).build())
                    .setNewTimestamp(newTs)
                    .build())
            .build();
  }
  public static CommunicationProtocol.Message createSendNack(String systemId,CommunicationProtocol.ProcessId destination,String toAbstraction){
    return  CommunicationProtocol.Message.newBuilder()
            .setType(CommunicationProtocol.Message.Type.PL_SEND)
            .setToAbstractionId(toAbstraction+".pl")
            .setSystemId(systemId)
            .setPlSend(CommunicationProtocol.PlSend.newBuilder()
                    .setMessage(CommunicationProtocol.Message.newBuilder()
                            .setToAbstractionId(toAbstraction)
                            .setType(CommunicationProtocol.Message.Type.EC_INTERNAL_NACK)
                            .setEcInternalNack(CommunicationProtocol.EcInternalNack.newBuilder().build())
                            .build())
                    .setDestination(destination)
                    .build())
            .build();
  }
}
