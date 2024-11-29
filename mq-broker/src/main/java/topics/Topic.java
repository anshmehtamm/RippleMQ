package topics;

import java.io.Serializable;

/**
 * Topic represents an individual topic with its name, number of partitions, and replication factor.
 */
public class Topic implements Serializable {

  private static final long serialVersionUID = 1L;

  private String name;
  private int partitions;
  private int replicationFactor;

  /**
   * Default constructor.
   */
  public Topic() {
  }

  /**
   * Constructor with parameters.
   *
   * @param name              The name of the topic
   * @param partitions        The number of partitions for the topic
   * @param replicationFactor The replication factor for the topic
   */
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

  @Override
  public String toString() {
    return "Topic{name='" + name + "', partitions=" + partitions +
            ", replicationFactor=" + replicationFactor + "}";
  }
}
