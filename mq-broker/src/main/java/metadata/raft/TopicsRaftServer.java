package metadata.raft;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import config.ClusterConfig;
import metadata.PartitionManager;
import metadata.raft.request.processor.ConsumerOffsetUpdateRequestProcessor;
import metadata.raft.request.processor.MessageAppendRequestProcessor;
import metadata.raft.request.processor.MessageBatchReadRequestProcessor;
import metadata.raft.request.TopicsClosure;
// import TopicsRequest from org.example.mq-co
import metadata.raft.request.processor.PartitionLeaderUpdateRequestProcessor;
import metadata.raft.request.processor.TopicsRequestProcessor;
import metadata.model.Topic;
import request.metadata.TopicsRequest;

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
  RpcServer rpcServer;

  private static final String GROUP_ID = "topics_cluster";
  private static final String STORAGE_DIR = "/tmp/raft/topics";

  private PeerId selfPeerId;
  private Configuration initialConf;
  private List<PeerId> peers;
  private Map<String, PeerId> brokerIdToPeerId = new HashMap<>();
  private ConsumerOffsetUpdateRequestProcessor consumerOffsetUpdateRequestProcessor;
  private MessageBatchReadRequestProcessor messageBatchReadRequestProcessor;
  private MessageAppendRequestProcessor messageAppendRequestProcessor;
  private PartitionLeaderUpdateRequestProcessor partitionLeaderUpdateRequestProcessor;

  /**
   * Constructor that initializes the PartitionManager and sets up Raft.
   *
   * @param clusterConfig The cluster configuration
   * @param brokerId      The ID of this broker
   * @throws IOException If an I/O error occurs during setup
   */
  public TopicsRaftServer(ClusterConfig clusterConfig, String brokerId) throws IOException {
    setSelfPeerId(clusterConfig, brokerId);



    setupRaft(clusterConfig, brokerId);
    this.partitionManager = new PartitionManager(selfPeerId, rpcServer);
    this.partitionManager.setTopicsRaftServer(this);
    this.stateMachine.setPartitionManager(partitionManager);
    this.consumerOffsetUpdateRequestProcessor.setPartitionManager(partitionManager);
    this.messageBatchReadRequestProcessor.setPartitionManager(partitionManager);
    this.messageAppendRequestProcessor.setPartitionManager(partitionManager);
    this.partitionLeaderUpdateRequestProcessor.setPartitionManager(partitionManager);
  }

  private void setSelfPeerId(ClusterConfig clusterConfig, String brokerId) {
    // Parse self PeerId
    ClusterConfig.BrokerConfig selfBroker = clusterConfig.getBrokerConfig(brokerId);
    if (selfBroker == null) {
      throw new IllegalArgumentException("Broker with ID " + brokerId + " not found in the cluster configuration.");
    }

    selfPeerId = new PeerId(selfBroker.getHostname(), selfBroker.getPort());
    System.out.println("Self PeerId: " + selfPeerId);
  }

  /**
   * Starts the Raft node and initializes monitoring.
   *
   * @throws IOException If the node fails to start
   */
  public void start() throws IOException {
    this.node = this.raftGroupService.start();
    // Start monitoring membership changes
    startMembershipMonitor();
  }

  private void setupRaft(ClusterConfig clusterConfig, String brokerId) throws IOException {
    // Ensure storage directories exist
    FileUtils.forceMkdir(new File(STORAGE_DIR));
    // Create and configure RPC server
    this.rpcServer = RaftRpcServerFactory.createRaftRpcServer(selfPeerId.getEndpoint());

    // Register the TopicsRequestProcessor
    rpcServer.registerProcessor(new TopicsRequestProcessor(this));
    consumerOffsetUpdateRequestProcessor = new ConsumerOffsetUpdateRequestProcessor();
    rpcServer.registerProcessor(consumerOffsetUpdateRequestProcessor);

    messageBatchReadRequestProcessor = new MessageBatchReadRequestProcessor();
    rpcServer.registerProcessor(messageBatchReadRequestProcessor);

    messageAppendRequestProcessor = new MessageAppendRequestProcessor();
    rpcServer.registerProcessor(messageAppendRequestProcessor);

    partitionLeaderUpdateRequestProcessor = new PartitionLeaderUpdateRequestProcessor(this);
    rpcServer.registerProcessor(partitionLeaderUpdateRequestProcessor);

    // Initialize the state machine with a reference to PartitionManager
    this.stateMachine = new TopicsStateMachine(this.partitionManager, selfPeerId);
    // Configure Raft node options
    NodeOptions nodeOptions = new NodeOptions();
    StringBuilder peerString = buildPeerString(clusterConfig);
    initialConf = new Configuration();
    if (!initialConf.parse(peerString.toString())) {
      throw new IllegalArgumentException("Failed to parse initial cluster configuration.");
    }
    nodeOptions.setElectionTimeoutMs(3000); // Increased for stability
    nodeOptions.setInitialConf(initialConf);
    nodeOptions.setFsm(this.stateMachine);
    nodeOptions.setRaftMetaUri(STORAGE_DIR + File.separator + "raft_meta");
    nodeOptions.setLogUri(STORAGE_DIR + File.separator + "raft_log");
    nodeOptions.setSnapshotUri(STORAGE_DIR + File.separator + "raft_snapshot");
    // Initialize RaftGroupService
    this.raftGroupService = new RaftGroupService(GROUP_ID, selfPeerId, nodeOptions, rpcServer);
    this.cliService = RaftServiceFactory.createAndInitCliService(new CliOptions());
  }

  private StringBuilder buildPeerString(ClusterConfig clusterConfig) {
    // Build initial cluster configuration
    StringBuilder peerString = new StringBuilder();
    this.peers = new ArrayList<>();
    for (ClusterConfig.BrokerConfig broker : clusterConfig.getBrokers()) {
      PeerId peerId = new PeerId(broker.getHostname(), broker.getPort());
      peers.add(peerId);
      brokerIdToPeerId.put(broker.getId(), peerId);
      peerString.append(peerId.toString()).append(",");
    }
    if (peerString.length() > 0) {
      peerString.setLength(peerString.length() - 1); // Remove trailing comma
    }
    return peerString;
  }

  public PeerId getSelfPeerId() {
    return selfPeerId;
  }

  public List<PeerId> getCurrentPeers() {
    return cliService.getAlivePeers(GROUP_ID, initialConf);
  }

  public Node getNode() {
    return node;
  }

  public TopicsStateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * Updates the topics via Raft.
   *
   * @param updatedTopics The updated list of topics
   */
  public synchronized void updateTopics(List<Topic> updatedTopics) {

    // Create a TopicsRequest with the updated topics
    TopicsRequest request = new TopicsRequest(updatedTopics);
    // Apply the request to the Raft node
    // Create a closure to handle the result
    TopicsClosure closure = new TopicsClosure(this, request, null);
    try {
      // Serialize the request
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(request);
      oos.flush();
      byte[] data = baos.toByteArray();

      // Apply the request to the Raft node
      this.node.apply(new Task(ByteBuffer.wrap(data), closure));
    } catch (IOException e) {
      e.printStackTrace();
      // Handle exception
    }
  }

  private void startMembershipMonitor() {
    // Implement membership monitoring and call partitionManager.handleMembershipChange()
    // For demonstration, we'll simulate periodic checks
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          List<PeerId> currentMembers = getCurrentPeers();
          partitionManager.handleMembershipChange(currentMembers);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }, 5000, 10000); // Start after 5 seconds, repeat every 10 seconds
  }

  /**
   * Shuts down the Raft server and scheduler gracefully.
   */
  public void shutdown() {
    if (raftGroupService != null) {
      raftGroupService.shutdown();
    }
  }
}
