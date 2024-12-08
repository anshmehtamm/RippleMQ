package partition.selector;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import metadata.model.PartitionAssignment;

public class RoundRobinSelector implements PartitionSelector {
  private static final Logger logger = LoggerFactory.getLogger(RoundRobinSelector.class);

  private final ConcurrentHashMap<String, AtomicInteger> topicCounters = new ConcurrentHashMap<>();

  @Override
  public PartitionAssignment selectPartition(String topic, List<PartitionAssignment> partitions) {
    logger.debug("Selecting partition for topic: {} from {} available partitions",
            topic, partitions.size());

    AtomicInteger counter = topicCounters.computeIfAbsent(topic, t -> {
      logger.trace("Creating new counter for topic: {}", t);
      return new AtomicInteger(0);
    });

    int index = Math.abs(counter.getAndIncrement()) % partitions.size();
    PartitionAssignment selected = partitions.get(index);

    logger.debug("Selected partition {} for topic {} (counter: {})",
            selected.getPartitionId(), topic, counter.get());

    return selected;
  }
}