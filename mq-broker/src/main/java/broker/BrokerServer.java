package broker;

import java.io.IOException;

import config.ClusterConfig;

/**
 * BrokerServer handles the main broker operations, including managing topics and partitions,
 * and handling producer and consumer requests.
 */
public class BrokerServer {

  private final ClusterConfig clusterConfig;
  private final BrokerClusterManager clusterManager;

  /**
   * Constructor that initializes the BrokerServer.
   *
   * @param clusterConfig The cluster configuration
   * @param brokerId      The ID of this broker
   * @throws IOException If an I/O error occurs during initialization
   */
  public BrokerServer(ClusterConfig clusterConfig, String brokerId) throws IOException {
    this.clusterConfig = clusterConfig;
    this.clusterManager = new BrokerClusterManager(brokerId);
  }

  /**
   * Starts the BrokerServer, including the Raft server and other components.
   *
   * @throws IOException If an I/O error occurs during startup
   */
  public void start() throws IOException {
    // Start the Raft server
    clusterManager.start();

    // TODO: Initialize other components such as partition manager, producer/consumer handlers
    // Example:
    // PartitionManager partitionManager = new PartitionManager(clusterManager.getRaftServer());
    // partitionManager.initialize();

    System.out.println("BrokerServer started successfully.");
  }

  /**
   * Stops the BrokerServer gracefully.
   */
  public void stop() {
    // TODO: Shutdown other components gracefully
    // Example:
    // partitionManager.shutdown();

    // Shutdown the Raft server
    clusterManager.stop();

    System.out.println("BrokerServer stopped successfully.");
  }
}
