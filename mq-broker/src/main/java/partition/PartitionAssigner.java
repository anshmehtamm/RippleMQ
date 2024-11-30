package partition;

import com.alipay.sofa.jraft.entity.PeerId;

import java.util.*;

/**
 * PartitionAssigner handles partition assignment to brokers.
 */
public class PartitionAssigner {

  /**
   * Assigns partitions to brokers based on the current cluster state.
   *
   * @param topics  The list of topics to assign partitions for
   * @param peers   The list of PeerIds currently in the cluster
   * @return The updated list of topics with partition assignments
   */
  public List<Topic> assignPartitions(List<Topic> topics, List<PeerId> peers) {
    if (peers == null || peers.isEmpty()) {
      throw new IllegalArgumentException("Peer list cannot be null or empty");
    }

    Map<String, Integer> brokerLoad = new HashMap<>();
    for (PeerId peer : peers) {
      brokerLoad.put(peer.toString(), 0);
    }

    for (Topic topic : topics) {
      List<PartitionAssignment> assignments = new ArrayList<>();
      int numPartitions = topic.getPartitions();
      int replicationFactor = topic.getReplicationFactor();

      if (replicationFactor > peers.size()) {
        throw new IllegalArgumentException("Replication factor cannot be greater than the number of brokers");
      }

      // Create a list of broker IDs
      List<String> brokerIds = new ArrayList<>();
      for (PeerId peer : peers) {
        brokerIds.add(peer.toString());
      }

      // For each partition, assign it to replicationFactor brokers
      for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
        // Sort brokers by current load (ascending)
        brokerIds.sort(Comparator.comparingInt(brokerLoad::get));

        List<String> assignedBrokers = new ArrayList<>();

        // Assign to the least loaded brokers
        for (String brokerId : brokerIds) {
          if (assignedBrokers.size() < replicationFactor) {
            assignedBrokers.add(brokerId);
            brokerLoad.put(brokerId, brokerLoad.get(brokerId) + 1);
          } else {
            break;
          }
        }

        assignments.add(new PartitionAssignment(partitionId, assignedBrokers));
      }

      topic.setPartitionAssignments(assignments);
    }

    return topics;
  }
}
