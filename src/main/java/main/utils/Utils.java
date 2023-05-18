package main.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import main.CommunicationProtocol;
import main.domain.NNARStruct;

public final class Utils {

  public static String getRegisterIdFromAbstraction(String abstraction) {
    System.out.println(abstraction);
    int start = abstraction.indexOf('[') + 1;
    int end = abstraction.indexOf(']');
    int length = end - start;
    return abstraction.substring(start, end);
  }
  public static String getRegisterIdFromAbstraction(String abstraction, CommunicationProtocol.Message message) {
    System.out.println("zzzzzzzzzzzzzzzz\n" + abstraction + "\n" + message + "zzzzzzzzzzzzzzzz\n");
    int start = abstraction.indexOf('[') + 1;
    int end = abstraction.indexOf(']');
    int length = end - start;
    return abstraction.substring(start, end);
  }

  public static String getEpConsensusId(String abstraction) {
    int start = abstraction.indexOf("ep[") + 3;
    int end = abstraction.indexOf(']',start+1);
    int length = end - start;
    return abstraction.substring(start, end);
  }

  public static NNARStruct getValueStructWithHighestTimestamp(NNARStruct[] list) {
    NNARStruct biggest = new NNARStruct();
    for (NNARStruct NNARStruct : list) {
      if(NNARStruct != null){
        if(NNARStruct.getTs() > biggest.getTs() || (NNARStruct.getTs() == biggest.getTs() && NNARStruct.getWr() > biggest.getWr())){
          biggest = NNARStruct;
        }
      }
    }
    return biggest;
  }
  public static int getIndexByPort(List<CommunicationProtocol.ProcessId> processIdList, int port) {
    for (int i = 0; i < processIdList.size(); i++) {
      if (processIdList.get(i).getPort() == port) return i;
    }
    return -1;
  }

  public static boolean isMyMessage(String id, String myid) {
    String[] targets = id.split("\\.");
    String[] myIdSplit = myid.split("\\.");

    return Objects.equals(targets[targets.length - 1], myIdSplit[myIdSplit.length - 1]);

  }

  public static boolean areEqualProcesses(CommunicationProtocol.ProcessId process1, CommunicationProtocol.ProcessId process2)
  {
    if (!process1.getHost().equals(process2.getHost())) return false;
    return process1.getPort() == process2.getPort();
  }

  public static boolean isIntersection(List<CommunicationProtocol.ProcessId> firstList, List<CommunicationProtocol.ProcessId> secondList){
    return Intersection(firstList, secondList).size() > 0;

  }

  public static List<CommunicationProtocol.ProcessId> Intersection(List<CommunicationProtocol.ProcessId> list1, List<CommunicationProtocol.ProcessId> list2)
  {
    List<CommunicationProtocol.ProcessId> intersection = new ArrayList<>();
    list1.forEach(list1Process ->{
      list2.forEach(list2Process->{
        if(areEqualProcesses(list1Process,list2Process))
          intersection.add(list1Process);
      });
    });
    return intersection;
  }

  public static boolean CheckIfContainsProcessId(List<CommunicationProtocol.ProcessId> list, CommunicationProtocol.ProcessId process)
  {
    for (var listItem : list) {
        if(areEqualProcesses(listItem,process)){
          return true;
        }
    }
    return false;
  }

}
