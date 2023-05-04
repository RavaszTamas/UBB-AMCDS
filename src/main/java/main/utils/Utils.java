package main.utils;

import java.util.List;
import main.CommunicationProtocol;
import main.domain.NNARStruct;

public final class Utils {

  public static String getRegisterIdFromAbstraction(String abstraction) {
    int start = abstraction.indexOf('[') + 1;
    int end = abstraction.indexOf(']');
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
}
