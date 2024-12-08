package client;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import metadata.MetadataManager;
import metadata.model.PartitionAssignment;
import partition.selector.PartitionClient;
import partition.selector.PartitionSelector;
import partition.selector.RoundRobinSelector;

public class ProducerClientImpl implements ProducerClient {
  private static final Logger logger = LoggerFactory.getLogger(ProducerClientImpl.class);
  private static final int DEFAULT_REFRESH_INTERVAL_MS = 10_000;

  private final String clientId;
  private final PartitionSelector partitionSelector;
  private final MetadataManager metadataManager;
  private final ScheduledExecutorService metadataRefreshScheduler;
  private final PartitionClient partitionClient;
  private final List<String> brokerPeerIds;

  public ProducerClientImpl(String clientId, List<String> brokerPeerIds, int metadataRefreshIntervalMs) {
    this.clientId = clientId;
    this.brokerPeerIds = brokerPeerIds;
    this.partitionSelector = new RoundRobinSelector();
    this.metadataManager = new MetadataManager(brokerPeerIds);
    this.partitionClient = new PartitionClient(metadataManager.getRpcClient());
    this.metadataRefreshScheduler = Executors.newSingleThreadScheduledExecutor();

    logger.info("Initializing ProducerClient with ID: {}", clientId);
    metadataManager.refreshMetadata();
    periodicMetadataRefresh(metadataRefreshIntervalMs);
  }

  public ProducerClientImpl(String clientId, List<String> brokerPeerIds) {
    this(clientId, brokerPeerIds, DEFAULT_REFRESH_INTERVAL_MS);
  }

  private void periodicMetadataRefresh(int metadataRefreshIntervalMs) {
    logger.debug("Setting up periodic metadata refresh every {} ms", metadataRefreshIntervalMs);
    this.metadataRefreshScheduler.scheduleAtFixedRate(() -> {
      try {
        logger.trace("Refreshing metadata");
        metadataManager.refreshMetadata();
      } catch (Exception e) {
        logger.error("Failed to refresh metadata", e);
      }
    }, 0, metadataRefreshIntervalMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void produce(String topic, String message) throws RuntimeException {
    logger.debug("Attempting to produce message to topic: {}", topic);
    List<PartitionAssignment> partitions = metadataManager.getPartitionsForTopic(topic);

    if (partitions == null) {
      logger.error("Topic not found: {}", topic);
      throw new RuntimeException("Topic not found: " + topic);
    }
    if (partitions.isEmpty()) {
      logger.error("No partitions found for topic: {}", topic);
      throw new RuntimeException("No partitions found for topic: " + topic);
    }

    PartitionAssignment partition = partitionSelector.selectPartition(topic, partitions);
    try {
      String leaderAddress = partition.getLeader();
      if (leaderAddress == null) {
        logger.error("No leader found for partition: {}", partition.getPartitionId());
        throw new RuntimeException("No leader found for partition: " + partition.getPartitionId());
      }

      String leaderPortModifiedAddress = getPortModifiedAddress(leaderAddress);
      logger.debug("Sending message to partition {} at address {}",
              partition.getPartitionId(), leaderPortModifiedAddress);

      partitionClient.sendMessage(leaderPortModifiedAddress, topic,
              partition.getPartitionId(), message);

      logger.debug("Successfully sent message to topic: {} partition: {}",
              topic, partition.getPartitionId());
    } catch (Exception e) {
      logger.error("Failed to send message to topic: {} partition: {}",
              topic, partition.getPartitionId(), e);
      throw new RuntimeException("Failed to send message", e);
    }
  }

  @Override
  public void close() {
    logger.info("Shutting down ProducerClient: {}", clientId);
    metadataRefreshScheduler.shutdown();
    partitionClient.close();
  }

  private String getPortModifiedAddress(String address) {
    logger.debug("Converting broker address: {}", address);
    String index = address.split("broker")[1];
    String modifiedAddress = brokerPeerIds.get(Integer.parseInt(String.valueOf(index.charAt(0))) - 1);
    logger.debug("Converted address to: {}", modifiedAddress);
    return modifiedAddress;
  }
}