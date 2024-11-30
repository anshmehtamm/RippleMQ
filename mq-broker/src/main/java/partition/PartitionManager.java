package partition;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.PeerId;

import java.util.*;

import config.ClusterConfigManager;
import metadata.raft.TopicsRaftServer;

/**
 * PartitionManager handles Raft events such as leader changes,
 * membership changes, and topic list updates.
 */
public class PartitionManager {

  private TopicsRaftServer topicsRaftServer;
  private List<PeerId> previousMembers = new ArrayList<>();

  public void setTopicsRaftServer(TopicsRaftServer topicsRaftServer) {
    this.topicsRaftServer = topicsRaftServer;
  }

  /**
   * Handles leader change events.
   *
   * @param newLeader The new leader's PeerId. Null if there's no leader.
   */
  public void handleLeaderChange(PeerId newLeader) {
    if (newLeader != null && newLeader.equals(topicsRaftServer.getSelfPeerId())) {
      // We are the leader
      System.out.println("PartitionManager: New leader elected - " + newLeader);
      // Get current topics
      List<Topic> topics = topicsRaftServer.getStateMachine().getTopics();
      if (topics.isEmpty()) {
        ClusterConfigManager configManager = ClusterConfigManager.getInstance();
        topics = configManager.getClusterConfig().getTopics();
      }
      // Get current cluster members
      List<PeerId> peers = topicsRaftServer.getCurrentPeers();
      // Assign partitions
      if (peers.equals(previousMembers)) {
        System.out.println("PartitionManager: Cluster membership unchanged. Skipping partition reassignment.");
        return;
      }
      PartitionAssigner partitionAssigner = new PartitionAssigner();
      List<Topic> updatedTopics = partitionAssigner.assignPartitions(topics, peers);
      // Update topics via Raft
      topicsRaftServer.updateTopics(updatedTopics);
    } else {
      // We are not the leader
    }
  }

  /**
   * Handles membership change events.
   *
   * @param currentMembers The current list of cluster members.
   */
  public void handleMembershipChange(List<PeerId> currentMembers) {


    // Check if we are the leader
    Node node = topicsRaftServer.getNode();
    if (node != null && node.isLeader() && !currentMembers.equals(previousMembers)) {
      // Determine if a broker has died or a new broker has joined
      if (previousMembers != null) {
        System.out.println("PartitionManager: Cluster membership changed. Current members:");
        for (PeerId member : currentMembers) {
          System.out.println("\t" + member);
        }
        Set<PeerId> previousSet = new HashSet<>(previousMembers);
        Set<PeerId> currentSet = new HashSet<>(currentMembers);

        Set<PeerId> added = new HashSet<>(currentSet);
        added.removeAll(previousSet);

        Set<PeerId> removed = new HashSet<>(previousSet);
        removed.removeAll(currentSet);

        if (!added.isEmpty() || !removed.isEmpty()) {
          System.out.println("PartitionManager: Membership change detected.");
          // Reassign partitions
          List<Topic> topics = topicsRaftServer.getStateMachine().getTopics();
          if (topics.isEmpty()) {
            ClusterConfigManager configManager = ClusterConfigManager.getInstance();
            topics = configManager.getClusterConfig().getTopics();
          }
          PartitionAssigner partitionAssigner = new PartitionAssigner();
          List<Topic> updatedTopics = partitionAssigner.assignPartitions(topics, currentMembers);
          // Update topics via Raft
          topicsRaftServer.updateTopics(updatedTopics);

        }
      }
      previousMembers = new ArrayList<>(currentMembers);
    } else {;
    }
  }

  /**
   * Handles topic list change events.
   *
   * @param newTopics The updated list of topics.
   */
  public void handleTopicListChange(List<Topic> newTopics) {
    System.out.println("PartitionManager: Topic list updated. New topics:");
    for (Topic topic : newTopics) {
      System.out.println("\t" + topic.toString());
    }
    // No additional logic needed here for now
  }
}
