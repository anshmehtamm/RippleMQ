package app;

import java.io.IOException;

import broker.BrokerServer;
import config.ClusterConfigManager;

public class ApplicationMain {

  public static void main(String[] args) throws IOException {
    /**
     * Accepted arguments
     *
     * - id: The id of the broker
     * - config: The path to the cluster configuration file
     * - host: The hostname of the broker
     * - port: The port of the broker
     * - debug level: The debug level of the broker
     *
     * Default values:
     * - id: identified from the configuration file
     * - config: mq-broker/config/cluster_config.yaml
     * - host: identified from the configuration file
     * - port: identified from the configuration file
     */
    setSystemProperties();

    if (args.length < 1) {
      System.err.println("Usage: java -jar broker-server.jar <brokerId>");
      System.exit(1);
    }

    String brokerId = args[1];

    // Load cluster configuration
    ClusterConfigManager clusterConfigManager = ClusterConfigManager.getInstance();
    clusterConfigManager.loadClusterConfig();

    // Instantiate BrokerServer
    BrokerServer brokerServer = new BrokerServer(clusterConfigManager.getClusterConfig(), brokerId);

    // Start BrokerServer
    brokerServer.start();

    // Add shutdown hook to gracefully stop the server
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        brokerServer.stop();
        System.out.println("Broker server stopped.");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));

    System.out.println("Broker server started successfully.");
  }

  private static void setSystemProperties() {
    // Implement any required system property settings here
    // Example:
    // System.setProperty("logback.configurationFile", "path/to/logback.xml");
  }
}
