package metadata;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import broker.BrokerRpcClient;
import config.ClusterConfigManager;
import metadata.model.PartitionAssignment;
import metadata.model.Topic;
import metadata.raft.PartitionRaftServer;
import metadata.raft.TopicsRaftServer;

public class PartitionManager {
  private static final Logger logger = LoggerFactory.getLogger(PartitionManager.class);

  private TopicsRaftServer topicsRaftServer;
  private final List<PeerId> previousMembers = new ArrayList<>();
  private final PeerId selfPeerId;
  private final RpcServer rpcServer;
  private final Map<String, PartitionRaftServer> activePartitions = new HashMap<>();

  public PartitionManager(PeerId selfPeerId, RpcServer rpcServer) {
    this.selfPeerId = selfPeerId;
    this.rpcServer = rpcServer;
    logger.info("Initialized PartitionManager for peer: {}", selfPeerId);
  }

  public void setTopicsRaftServer(TopicsRaftServer topicsRaftServer) {
    this.topicsRaftServer = topicsRaftServer;
    logger.debug("Set TopicsRaftServer instance");
  }

  public void handleLeaderChange(PeerId newLeader) {
    logger.info("Handling leader change event. New leader: {}", newLeader);

    if (newLeader != null && newLeader.equals(topicsRaftServer.getSelfPeerId())) {
      logger.info("This node is now the leader");
      List<Topic> topics = topicsRaftServer.getStateMachine().getTopics();

      if (topics.isEmpty()) {
        logger.debug("No topics found in state machine, loading from config");
        ClusterConfigManager configManager = ClusterConfigManager.getInstance();
        topics = configManager.getClusterConfig().getTopics();
      }

      List<PeerId> peers = topicsRaftServer.getCurrentPeers();
      if (peers.equals(previousMembers)) {
        logger.debug("Cluster membership unchanged, skipping partition reassignment");
        return;
      }

      logger.info("Reassigning partitions due to membership change");
      previousMembers.clear();
      previousMembers.addAll(peers);

      PartitionAssigner partitionAssigner = new PartitionAssigner();
      List<Topic> updatedTopics = partitionAssigner.assignPartitions(topics, peers);

      logger.debug("Updating topics with new partition assignments");
      topicsRaftServer.updateTopics(updatedTopics);
    } else {
      logger.info("This node is not the leader");
    }
  }

  public void handleMembershipChange(List<PeerId> currentMembers) {
    logger.info("Handling membership change event");
    Node node = topicsRaftServer.getNode();

    if (node != null && node.isLeader() && !currentMembers.equals(previousMembers)) {
      logger.info("Processing membership change as leader");

      if (previousMembers != null) {
        Set<PeerId> previousSet = new HashSet<>(previousMembers);
        Set<PeerId> currentSet = new HashSet<>(currentMembers);

        Set<PeerId> added = new HashSet<>(currentSet);
        added.removeAll(previousSet);

        Set<PeerId> removed = new HashSet<>(previousSet);
        removed.removeAll(currentSet);

        if (!added.isEmpty() || !removed.isEmpty()) {
          logger.info("Membership changes detected - Added: {}, Removed: {}", added, removed);

          List<Topic> topics = topicsRaftServer.getStateMachine().getTopics();
          if (topics.isEmpty()) {
            logger.debug("Loading topics from cluster config");
            ClusterConfigManager configManager = ClusterConfigManager.getInstance();
            topics = configManager.getClusterConfig().getTopics();
          }

          PartitionAssigner partitionAssigner = new PartitionAssigner();
          List<Topic> updatedTopics = partitionAssigner.assignPartitions(topics, currentMembers);

          logger.debug("Updating topics with new partition assignments");
          topicsRaftServer.updateTopics(updatedTopics);
        }
      }
      previousMembers.clear();
      previousMembers.addAll(currentMembers);
    }
  }

  public void handleTopicListChange(List<Topic> newTopics) {
    logger.info("Processing topic list change");

    Set<String> partitionsToHandle = new HashSet<>();
    Map<String, List<PeerId>> partitionPeerMap = new HashMap<>();

    // Analyze new topic assignments
    for (Topic topic : newTopics) {
      if (topic.getPartitionAssignments() != null) {
        for (PartitionAssignment assignment : topic.getPartitionAssignments()) {
          String partitionGroupId = topic.getName() + "-" + assignment.getPartitionId();
          List<PeerId> partitionPeers = new ArrayList<>();

          for (String brokerPeerIdStr : assignment.getBrokerPeerIds()) {
            PeerId brokerPeerId = new PeerId();
            brokerPeerId.parse(brokerPeerIdStr);
            partitionPeers.add(brokerPeerId);
          }

          partitionPeerMap.put(partitionGroupId, partitionPeers);

          // Check if this node is assigned to this partition
          for (PeerId peer : partitionPeers) {
            if (peer.getEndpoint().equals(selfPeerId.getEndpoint())) {
              partitionsToHandle.add(partitionGroupId);
              logger.debug("This node will handle partition: {}", partitionGroupId);
              break;
            }
          }
        }
      }
    }

    // Stop unnecessary partitions
    Set<String> partitionsToStop = new HashSet<>(activePartitions.keySet());
    partitionsToStop.removeAll(partitionsToHandle);

    for (String partitionGroupId : partitionsToStop) {
      logger.info("Stopping partition: {}", partitionGroupId);
      stopPartition(partitionGroupId);
    }

    // Start new partitions
    for (String partitionGroupId : partitionsToHandle) {
      if (!activePartitions.containsKey(partitionGroupId)) {
        logger.info("Starting new partition: {}", partitionGroupId);
        try {
          startPartition(partitionGroupId, partitionPeerMap.get(partitionGroupId));
        } catch (IOException e) {
          logger.error("Failed to start partition: {}", partitionGroupId, e);
        }
      }
    }
  }

  private void startPartition(String partitionGroupId, List<PeerId> partitionPeers) throws IOException {
    logger.info("Starting partition {} with peers: {}", partitionGroupId, partitionPeers);

    PartitionRaftServer partitionRaftServer = new PartitionRaftServer(
            partitionGroupId, selfPeerId, partitionPeers, this.rpcServer, this);

    activePartitions.put(partitionGroupId, partitionRaftServer);
    partitionRaftServer.start();

    logger.info("Successfully started partition: {}", partitionGroupId);
  }

  private void stopPartition(String partitionGroupId) {
    logger.info("Stopping partition: {}", partitionGroupId);

    PartitionRaftServer partitionRaftServer = activePartitions.remove(partitionGroupId);
    if (partitionRaftServer != null) {
      partitionRaftServer.shutdown();
      logger.info("Successfully stopped partition: {}", partitionGroupId);
    }
  }

  public void shutdown() {
    logger.info("Shutting down PartitionManager");
    for (String partitionGroupId : new HashSet<>(activePartitions.keySet())) {
      stopPartition(partitionGroupId);
    }
    logger.info("PartitionManager shutdown complete");
  }

  public PartitionRaftServer getPartitionRaftServer(String partitionGroupId) {
    return activePartitions.get(partitionGroupId);
  }

  public synchronized boolean handlePartitionLeaderChange(String groupId, String leaderAddress, boolean redirect) {
    logger.info("Handling partition leader change for group: {}, leader: {}, redirect: {}",
            groupId, leaderAddress, redirect);

    Node node = topicsRaftServer.getNode();
    if (node != null && node.isLeader()) {
      logger.debug("Processing leader change as cluster leader");
      makeLeaderForPartition(groupId, leaderAddress);
      return true;
    } else if (!redirect) {
      logger.debug("Redirecting leader change request to cluster leader");
      redirectPartitionLeaderChangeToTopicLeader(groupId, leaderAddress);
      return true;
    }

    logger.debug("Cannot process leader change request");
    return false;
  }

  private void redirectPartitionLeaderChangeToTopicLeader(String groupId, String leaderAddress) {
    logger.debug("Redirecting partition leader change request for group: {}", groupId);

    new Thread(() -> {
      int retries = 0;
      while (retries < 3) {
        try {
          Node node = topicsRaftServer.getNode();
          PeerId leader = node.getLeaderId();
          logger.debug("Attempting to update partition leader (attempt {})", retries + 1);

          BrokerRpcClient.getInstance(selfPeerId).updatePartitionLeader(
                  leader.getEndpoint(), groupId, leaderAddress);

          logger.info("Successfully redirected leader change request");
          break;
        } catch (RuntimeException e) {
          logger.warn("Failed to redirect leader change request (attempt {})", retries + 1, e);
          retries++;
          try {
            Thread.sleep(3000);
          } catch (InterruptedException ex) {
            logger.error("Sleep interrupted during retry", ex);
          }
        }
      }
    }).start();
  }

  public synchronized void handlePartitionLeaderChange(String groupId) {
    String leaderAddress = selfPeerId.toString();
    logger.info("Handling partition leader change for group: {} with leader: {}",
            groupId, leaderAddress);
    handlePartitionLeaderChange(groupId, leaderAddress, false);
  }

  private synchronized void makeLeaderForPartition(String groupId, String leaderAddress) {
    List<Topic> topics = topicsRaftServer.getStateMachine().getTopics();
    String topicName = groupId.split("-")[0];
    int partitionId = Integer.parseInt(groupId.split("-")[1]);
    boolean updated = false;
    for (Topic topic : topics) {
      if (topic.getName().equals(topicName)) {
        for (PartitionAssignment assignment : topic.getPartitionAssignments()) {
          if (assignment.getPartitionId() == partitionId) {
            assignment.setLeader(leaderAddress);
            topicsRaftServer.updateTopics(topics);
            updated = true;
            break;
          }
        }
      }
    }
    if (!updated) {
      System.err.println("PartitionManager: Failed to update leader for partition " + groupId);
    }
  }

}