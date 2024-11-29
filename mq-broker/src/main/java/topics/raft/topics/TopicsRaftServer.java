package topics.raft.topics;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.conf.Configuration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import broker.BrokerInfo;
import topics.PartitionManager;
import topics.raft.topics.request.TopicsRequestProcessor;

/**
 * TopicsRaftServer sets up and manages the Raft server,
 * integrates the PartitionManager, and monitors membership changes.
 */
public class TopicsRaftServer {

  private RaftGroupService raftGroupService;
  private Node node;
  private TopicsStateMachine stateMachine;
  private PartitionManager partitionManager;

  private CliService cliService;

  private static final String GROUP_ID = "topics_cluster";
  private static final String STORAGE_DIR = "/tmp/raft/topics";

  // Scheduled executor for membership monitoring
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private List<PeerId> previousMembers;
  private Configuration initialConf;

  /**
   * Constructor that initializes the PartitionManager and sets up Raft.
   *
   * @param peers     List of all brokers in the cluster
   * @param brokerId The ID of this broker
   * @throws IOException If an I/O error occurs during setup
   */
  public TopicsRaftServer(List<BrokerInfo> peers, String brokerId) throws IOException {
    this.partitionManager = new PartitionManager();
    setupRaft(peers, brokerId);
  }

  /**
   * Starts the Raft node and initializes monitoring.
   *
   * @throws IOException If the node fails to start
   */
  public void start() throws IOException {
    this.node = this.raftGroupService.start();
    // Notify PartitionManager about the initial leader
    PeerId leader = this.node.getLeaderId();
    if (leader != null) {
      partitionManager.handleLeaderChange(leader);
    } else {
      partitionManager.handleLeaderChange(null);
    }

    // Start monitoring membership changes
    startMembershipMonitor();
  }

  private void setupRaft(List<BrokerInfo> peers, String brokerId) throws IOException {
    // Ensure storage directories exist
    FileUtils.forceMkdir(new File(STORAGE_DIR));

    // Parse self PeerId
    BrokerInfo selfBroker = getSelfBroker(peers, brokerId);
    if (selfBroker == null) {
      throw new IllegalArgumentException("Broker with ID " + brokerId + " not found in the cluster configuration.");
    }

    PeerId selfPeerId = new PeerId();
    boolean parsed = selfPeerId.parse(selfBroker.getHostname() + ":" + selfBroker.getPort());
    if (!parsed) {
      throw new IllegalArgumentException("Failed to parse PeerId for self: " + selfBroker.getHostname() + ":" + selfBroker.getPort());
    }

    // Create and configure RPC server
    final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(selfPeerId.getEndpoint());

    // Register the TopicsRequestProcessor
    rpcServer.registerProcessor(new TopicsRequestProcessor(this));

    // Initialize the state machine with a reference to PartitionManager
    this.stateMachine = new TopicsStateMachine(this.partitionManager, selfPeerId);

    // Configure Raft node options
    NodeOptions nodeOptions = new NodeOptions();
    nodeOptions.setElectionTimeoutMs(3000); // Increased for stability

    // Build initial cluster configuration
    StringBuilder peerString = new StringBuilder();
    for (BrokerInfo peer : peers) {
      peerString.append(peer.getHostname()).append(":").append(peer.getPort()).append(",");
    }
    if (peerString.length() > 0) {
      peerString.setLength(peerString.length() - 1); // Remove trailing comma
    }
    Configuration initialConf = new Configuration();
    if (!initialConf.parse(peerString.toString())) {
      throw new IllegalArgumentException("Failed to parse initial cluster configuration.");
    }

    nodeOptions.setInitialConf(initialConf);
    nodeOptions.setFsm(this.stateMachine);
    nodeOptions.setRaftMetaUri(STORAGE_DIR + File.separator + "raft_meta");
    nodeOptions.setLogUri(STORAGE_DIR + File.separator + "raft_log");
    nodeOptions.setSnapshotUri(STORAGE_DIR + File.separator + "raft_snapshot");

    // Initialize RaftGroupService
    this.raftGroupService = new RaftGroupService(GROUP_ID, selfPeerId, nodeOptions, rpcServer);
    this.cliService = RaftServiceFactory.createAndInitCliService(new CliOptions());
    this.initialConf = initialConf;

  }

  /**
   * Starts a scheduled task to monitor membership changes.
   */
  private void startMembershipMonitor() {
    scheduler.scheduleAtFixedRate(() -> {
      try {
        List<PeerId> currentMembers = getAliveNodes();
        if (!currentMembers.equals(previousMembers)) {
          previousMembers = currentMembers;
          partitionManager.handleMembershipChange(currentMembers);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, 10, 10, TimeUnit.SECONDS); // Check every 10 seconds
  }

  /**
   * Retrieves the list of currently alive nodes in the cluster.
   *
   * @return List of PeerIds representing alive nodes
   */
  private List<PeerId> getAliveNodes() {
    return cliService.getAlivePeers(GROUP_ID, initialConf);
  }

  /**
   * Retrieves the BrokerInfo for the given broker ID.
   *
   * @param brokers  List of all brokers
   * @param brokerId The ID of the broker to find
   * @return The corresponding BrokerInfo, or null if not found
   */
  private BrokerInfo getSelfBroker(List<BrokerInfo> brokers, String brokerId) {
    for (BrokerInfo broker : brokers) {
      if (broker.getId().equals(brokerId)) {
        return broker;
      }
    }
    return null;
  }

  /**
   * Retrieves the Raft node.
   *
   * @return Raft Node
   */
  public Node getNode() {
    return node;
  }

  /**
   * Retrieves the state machine.
   *
   * @return TopicsStateMachine
   */
  public TopicsStateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * Retrieves the Raft group service.
   *
   * @return RaftGroupService
   */
  public RaftGroupService getRaftGroupService() {
    return raftGroupService;
  }

  /**
   * Retrieves the PartitionManager.
   *
   * @return PartitionManager
   */
  public PartitionManager getPartitionManager() {
    return partitionManager;
  }

  /**
   * Shuts down the Raft server and scheduler gracefully.
   */
  public void shutdown() {
    if (raftGroupService != null) {
      raftGroupService.shutdown();
    }
    if (scheduler != null && !scheduler.isShutdown()) {
      scheduler.shutdown();
    }
  }
}
