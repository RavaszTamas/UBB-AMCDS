package main;

import main.server.Process;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        String hubIp = args[0];
        int hubPort = Integer.parseInt(args[1]);
        String myIp = args[2];
        String owner = args[3];

        List<Thread> processes = new LinkedList<>();
        final int[] idx = {0};
        Arrays.stream(args).skip(4).forEach((port)->{
            processes.add(new Thread(new Process(hubIp,hubPort,myIp,Integer.parseInt(port),owner, idx[0] + 1)));
            idx[0] += 1;
        });

        processes.forEach(Thread::start);

    }
}