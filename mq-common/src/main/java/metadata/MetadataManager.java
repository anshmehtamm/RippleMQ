package metadata;

import com.alipay.sofa.jraft.rpc.RpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import metadata.model.PartitionAssignment;
import metadata.model.Topic;

public class MetadataManager {
  private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);

  private final List<Topic> topics;
  private final MetadataClient metadataClient;

  public MetadataManager(List<String> brokerAddresses) {
    this.topics = new ArrayList<>();
    this.metadataClient = new MetadataClient(brokerAddresses);
    logger.info("Initialized MetadataManager with {} broker addresses", brokerAddresses.size());
    logger.debug("Broker addresses: {}", brokerAddresses);
  }

  public synchronized void refreshMetadata() {
    logger.debug("Starting metadata refresh");
    List<Topic> fetchedTopics = metadataClient.fetchMetadata();

    this.topics.clear();
    this.topics.addAll(fetchedTopics);

    logger.info("Metadata refreshed, found {} topics", fetchedTopics.size());

    if (logger.isDebugEnabled()) {
      for (Topic topic : fetchedTopics) {
        logger.debug("Topic details: {}", topic);
      }
    }
  }

  public List<PartitionAssignment> getPartitionsForTopic(String topicName) {
    logger.debug("Getting partitions for topic: {}", topicName);

    if (topics.isEmpty()) {
      logger.debug("No topics in cache, fetching metadata");
      metadataClient.fetchMetadata();
    }

    for (Topic topic : topics) {
      if (topic.getName().equals(topicName)) {
        List<PartitionAssignment> partitions = topic.getPartitionAssignments();
        logger.debug("Found {} partitions for topic: {}",
                partitions != null ? partitions.size() : 0, topicName);
        return partitions;
      }
    }

    logger.warn("No partitions found for topic: {}", topicName);
    return null;
  }

  public RpcClient getRpcClient() {
    return metadataClient.getRpcClient();
  }
}