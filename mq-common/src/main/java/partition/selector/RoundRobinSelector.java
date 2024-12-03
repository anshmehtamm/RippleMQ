package partition.selector;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import metadata.model.PartitionAssignment;

public class RoundRobinSelector implements PartitionSelector{

  private final ConcurrentHashMap<String, AtomicInteger> topicCounters = new ConcurrentHashMap<>();

  @Override
  public PartitionAssignment selectPartition(String topic, List<PartitionAssignment> partitions) {
    AtomicInteger counter = topicCounters.computeIfAbsent(topic, t -> new AtomicInteger(0));
    int index = Math.abs(counter.getAndIncrement()) % partitions.size();
    return partitions.get(index);
  }
}
