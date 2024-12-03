package partition.selector;

import java.util.List;

import metadata.model.PartitionAssignment;

public interface PartitionSelector {


  public PartitionAssignment selectPartition(String topic, List<PartitionAssignment> partitionIds);
}
