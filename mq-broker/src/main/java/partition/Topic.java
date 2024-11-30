package partition;

import java.io.Serializable;
import java.util.List;

import partition.PartitionAssignment;

/**
 * Topic represents an individual topic with its name, number of partitions, replication factor,
 * and partition assignments.
 */
public class Topic implements Serializable {

  private static final long serialVersionUID = 1L;

  private String name;
  private int partitions;
  private int replicationFactor;
  private List<PartitionAssignment> partitionAssignments;

  // Constructors

  public Topic() {
  }

  public Topic(String name, int partitions, int replicationFactor) {
    this.name = name;
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
  }

  // Getters and Setters

  public String getName() {
    return name;
  }

  public int getPartitions() {
    return partitions;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public void setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public List<PartitionAssignment> getPartitionAssignments() {
    return partitionAssignments;
  }

  public void setPartitionAssignments(List<PartitionAssignment> partitionAssignments) {
    this.partitionAssignments = partitionAssignments;
  }

  @Override
  public String toString() {
    return "Topic{name='" + name + "', partitions=" + partitions +
            ", replicationFactor=" + replicationFactor +
            ", partitionAssignments=" + partitionAssignments + "}";
  }
}
