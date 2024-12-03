package metadata.model;

import java.io.Serializable;
import java.util.List;

/**
 * PartitionAssignment represents the assignment of a partition to brokers.
 */
public class PartitionAssignment implements Serializable {

  private static final long serialVersionUID = 1L;

  private int partitionId;
  private List<String> brokerPeerIds; // List of PeerIds in string format assigned to this partition

  private String leaderId; // Leader PeerId for this partition

  public PartitionAssignment(int partitionId, List<String> brokerPeerIds) {
    this.partitionId = partitionId;
    this.brokerPeerIds = brokerPeerIds;
  }

  // Getters and Setters

  public int getPartitionId() {
    return partitionId;
  }

  public List<String> getBrokerPeerIds() {
    return brokerPeerIds;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public void setBrokerPeerIds(List<String> brokerPeerIds) {
    this.brokerPeerIds = brokerPeerIds;
  }

  public String getLeader() {
    return leaderId;
  }

  public void setLeader(String leaderId) {
    this.leaderId = leaderId;
  }

  @Override
  public String toString() {
    return "PartitionAssignment{partitionId=" + partitionId + ", brokers=" + brokerPeerIds + ", " +
            "leaderId='" + leaderId + "'}";
  }
}
