package app;

public class Topic {
  private int replication_factor;
  private int partitions;
  private String name;

  public Topic() {}

  // Getters and Setters
  public int getReplication_factor() {
    return replication_factor;
  }

  public void setReplication_factor(int replication_factor) {
    if (replication_factor < 1) {
      throw new IllegalArgumentException("Replication factor must be greater than 0");
    }
    this.replication_factor = replication_factor;
  }

  public int getPartitions() {
    return partitions;
  }

  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
