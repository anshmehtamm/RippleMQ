package partition.raft;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

import partition.raft.request.PartitionRequestProcessor;

/**
 * PartitionRaftServer sets up and manages the Raft server for a partition.
 */
public class PartitionRaftServer {

  private final String groupId;
  private final String storageDir;

  private PeerId peerId;
  private List<PeerId> peers;

  private PartitionStateMachine stateMachine;
  private RaftGroupService raftGroupService;
  private CliService cliService;
  private Node node;
  private RpcServer rpcServer;

  /**
   * Constructor that initializes the PartitionRaftServer.
   *
   * @param groupId The Raft group ID (unique per partition)
   * @param selfId  The PeerId of this node
   * @param peers   The list of PeerIds in this Raft group
   * @throws IOException If an I/O error occurs during setup
   */
  public PartitionRaftServer(String groupId, PeerId selfId, List<PeerId> peers, RpcServer rpcServer) throws IOException {
    this.groupId = groupId;
    this.storageDir = "data/" + groupId;
    this.peerId = selfId;
    this.peers = peers;
    this.rpcServer = rpcServer;

    setupRaft();
    System.err.println("PartitionRaftServer for group " + groupId + " has been initialized.");
  }

  /**
   * Starts the Raft node.
   *
   * @throws IOException If the node fails to start
   */
  public void start() throws IOException {
    System.err.println("Starting PartitionRaftServer for group " + groupId);
    this.node = this.raftGroupService.start(false);
    System.err.println("PartitionRaftServer for group " + groupId + " has been started.");
  }

  private void setupRaft() throws IOException {
    // Ensure storage directories exist
    FileUtils.forceMkdir(new File(storageDir));

    // Register the PartitionRequestProcessor
    this.rpcServer.registerProcessor(new PartitionRequestProcessor(this, groupId));
    System.err.println("PartitionRequestProcessor for group " + groupId + " has been registered.");
    // Initialize the state machine
    this.stateMachine = new PartitionStateMachine(this.groupId);
    System.err.println("PartitionStateMachine for group " + groupId + " has been initialized.");
    // Configure Raft node options
    NodeOptions nodeOptions = new NodeOptions();
    Configuration initialConf = new Configuration(this.peers);

    nodeOptions.setElectionTimeoutMs(1000);
    nodeOptions.setInitialConf(initialConf);
    nodeOptions.setFsm(this.stateMachine);
    nodeOptions.setLogUri(storageDir + "/log");
    nodeOptions.setRaftMetaUri(storageDir + "/raft_meta");
    nodeOptions.setSnapshotUri(storageDir + "/snapshot");

    // Initialize RaftGroupService
    this.raftGroupService = new RaftGroupService(groupId, peerId, nodeOptions, rpcServer, true);
    System.err.println("RaftGroupService for group " + groupId + " has been initialized.");
  }

  /**
   * Shuts down the Raft node and releases resources.
   */
  public void shutdown() {
    if (raftGroupService != null) {
      raftGroupService.shutdown();
    }
    if (cliService != null) {
      cliService.shutdown();
    }
    System.out.println("PartitionRaftServer for group " + groupId + " has been shut down.");
  }

  // Getters
  public Node getNode() {
    return node;
  }

  public PartitionStateMachine getStateMachine() {
    return stateMachine;
  }

  public String getGroupId() {
    return groupId;
  }
}
