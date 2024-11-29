package broker;

import java.io.IOException;

import config.ClusterConfig;
import config.ClusterConfigManager;
import topics.raft.topics.TopicsRaftServer;

/**
 * BrokerClusterManager manages the Raft server for a broker.
 */
public class BrokerClusterManager {

  private TopicsRaftServer raftServer;

  /**
   * Constructor that initializes the Raft server.
   *
   * @param brokerId The ID of this broker
   * @throws IOException If an I/O error occurs during Raft server setup
   */
  public BrokerClusterManager(String brokerId) throws IOException {
    ClusterConfig config = ClusterConfigManager.getInstance().getClusterConfig();
    raftServer = new TopicsRaftServer(config.getBrokers(), brokerId);
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
