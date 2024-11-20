package app;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import messaging.connection.TCPConnectionManager;

public class ApplicationMain {


  public static void main(String[] args) {


    // read environment variables

    int brokerId = Integer.parseInt(System.getenv(EnvironmentVars.BROKER_ID.getName()));
    String brokerHostname = System.getenv(EnvironmentVars.BROKER_HOSTNAME.getName());
    String peerHostnames = System.getenv(EnvironmentVars.PEER_HOSTNAMES.getName());
    List<String> peers;
    if (peerHostnames != null && !peerHostnames.isEmpty()) {
      peers = Arrays.asList(peerHostnames.split(","));
    }

    // initialize the broker (bootstrap)








  }
}