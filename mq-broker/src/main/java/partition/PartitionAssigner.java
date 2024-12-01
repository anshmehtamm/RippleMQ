package partition;

import com.alipay.sofa.jraft.entity.PeerId;

import java.util.*;

/**
 * PartitionAssigner handles partition assignment to brokers,
 * aiming to minimize partition movement during cluster changes.
 */
public class PartitionAssigner {

  /**
   * Assigns partitions to brokers based on the current cluster state.
   * Keeps existing assignments when possible and reassigns partitions
   * from dead brokers to live brokers.
   *
   * @param topics  The list of topics to assign partitions for
   * @param currentPeers   The list of PeerIds currently in the cluster
   * @return The updated list of topics with partition assignments
   */
  public List<Topic> assignPartitions(List<Topic> topics, List<PeerId> currentPeers) {
    if (currentPeers == null || currentPeers.isEmpty()) {
      throw new IllegalArgumentException("Peer list cannot be null or empty");
    }

    // Convert currentPeers to a set of strings for easier comparison
    Set<String> liveBrokers = new HashSet<>();
    for (PeerId peer : currentPeers) {
      liveBrokers.add(peer.toString());
    }

    // Map to keep track of broker load (number of partitions assigned)
    Map<String, Integer> brokerLoad = new HashMap<>();
    for (String brokerId : liveBrokers) {
      brokerLoad.put(brokerId, 0);
    }

    for (Topic topic : topics) {
      List<PartitionAssignment> assignments = topic.getPartitionAssignments();
      int replicationFactor = topic.getReplicationFactor();

      if (replicationFactor > liveBrokers.size()) {
        throw new IllegalArgumentException("Replication factor cannot be greater than the number of live brokers");
      }

      // If there are no existing assignments, initialize them
      if (assignments == null || assignments.isEmpty()) {
        assignments = new ArrayList<>();
        topic.setPartitionAssignments(assignments);
        // Initialize assignments for all partitions
        for (int partitionId = 0; partitionId < topic.getPartitions(); partitionId++) {
          assignments.add(new PartitionAssignment(partitionId, new ArrayList<>()));
        }
      }

      // Reassign partitions as necessary
      for (PartitionAssignment assignment : assignments) {
        List<String> currentAssignment = assignment.getBrokerPeerIds();
        if (currentAssignment == null) {
          currentAssignment = new ArrayList<>();
          assignment.setBrokerPeerIds(currentAssignment);
        }

        // Remove dead brokers from the current assignment
        List<String> deadBrokers = new ArrayList<>();
        for (String brokerId : currentAssignment) {
          if (!liveBrokers.contains(brokerId)) {
            deadBrokers.add(brokerId);
          } else {
            // Broker is alive, update broker load
            brokerLoad.put(brokerId, brokerLoad.get(brokerId) + 1);
          }
        }
        currentAssignment.removeAll(deadBrokers);

        // Add new brokers to maintain replication factor
        while (currentAssignment.size() < replicationFactor) {
          String leastLoadedBroker = getLeastLoadedBroker(brokerLoad, currentAssignment);
          if (leastLoadedBroker == null) {
            // No more brokers to assign (should not happen)
            break;
          }
          currentAssignment.add(leastLoadedBroker);
          brokerLoad.put(leastLoadedBroker, brokerLoad.get(leastLoadedBroker) + 1);
        }
      }
    }

    return topics;
  }

  /**
   * Finds the live broker with the least load that is not already in the current assignment.
   *
   * @param brokerLoad        Map of broker IDs to their current load
   * @param currentAssignment List of broker IDs already assigned to the partition
   * @return The broker ID with the least load, or null if none are available
   */
  private String getLeastLoadedBroker(Map<String, Integer> brokerLoad, List<String> currentAssignment) {
    String leastLoadedBroker = null;
    int minLoad = Integer.MAX_VALUE;
    for (Map.Entry<String, Integer> entry : brokerLoad.entrySet()) {
      String brokerId = entry.getKey();
      int load = entry.getValue();
      if (!currentAssignment.contains(brokerId) && load < minLoad) {
        leastLoadedBroker = brokerId;
        minLoad = load;
      }
    }
    return leastLoadedBroker;
  }
}
