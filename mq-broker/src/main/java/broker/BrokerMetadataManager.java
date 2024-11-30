package broker;

import java.io.IOException;

import config.ClusterConfig;
import metadata.raft.TopicsRaftServer;


/**
 * BrokerMetadataManager manages the Raft server for a broker.
 */
public class BrokerMetadataManager {

  private TopicsRaftServer raftServer;

  /**
   * Constructor that initializes the Raft server.
   *
   * @param clusterConfig The cluster configuration
   * @param brokerId      The ID of this broker
   * @throws IOException If an I/O error occurs during Raft server setup
   */
  public BrokerMetadataManager(ClusterConfig clusterConfig, String brokerId) throws IOException {
    raftServer = new TopicsRaftServer(clusterConfig, brokerId);
  }

  /**
   * Starts the Raft server.
   *
   * @throws IOException If an I/O error occurs during server startup
   */
  public void start() throws IOException {
    raftServer.start();
  }

  /**
   * Shuts down the Raft server.
   */
  public void stop() {
    raftServer.shutdown();
  }

  /**
   * Retrieves the Raft server instance.
   *
   * @return TopicsRaftServer
   */
  public TopicsRaftServer getRaftServer() {
    return raftServer;
  }
}
