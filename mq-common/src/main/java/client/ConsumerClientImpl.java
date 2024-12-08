package client;

import java.util.List;
import java.util.concurrent.CompletableFuture;
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
import request.partition.MessageBatchReadResponse;

public class ConsumerClientImpl implements ConsumerClient {
  private static final Logger logger = LoggerFactory.getLogger(ConsumerClientImpl.class);
  private static final int DEFAULT_REFRESH_INTERVAL_MS = 10_000;
  private static final int DEFAULT_MAX_MESSAGES = 10;

  private final String clientId;
  private final PartitionSelector partitionSelector;
  private final MetadataManager metadataManager;
  private final ScheduledExecutorService metadataRefreshScheduler;
  private final PartitionClient partitionClient;
  private final List<String> brokerPeerIds;

  public ConsumerClientImpl(String clientId, List<String> brokerPeerIds, int metadataRefreshIntervalMs) {
    this.clientId = clientId;
    this.brokerPeerIds = brokerPeerIds;
    this.partitionSelector = new RoundRobinSelector();
    this.metadataManager = new MetadataManager(brokerPeerIds);
    this.partitionClient = new PartitionClient(metadataManager.getRpcClient());
    this.metadataRefreshScheduler = Executors.newSingleThreadScheduledExecutor();

    logger.info("Initializing ConsumerClient with ID: {}", clientId);
    logger.debug("Broker peer IDs: {}", brokerPeerIds);

    metadataManager.refreshMetadata();
    periodicMetadataRefresh(metadataRefreshIntervalMs);
  }

  public ConsumerClientImpl(String clientId, List<String> brokerPeerIds) {
    this(clientId, brokerPeerIds, DEFAULT_REFRESH_INTERVAL_MS);
  }

  private void periodicMetadataRefresh(int metadataRefreshIntervalMs) {
    logger.debug("Setting up periodic metadata refresh with interval: {}ms", metadataRefreshIntervalMs);
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
  public List<String> consume(String topic) {
    logger.debug("Attempting to consume messages from topic: {}", topic);

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
      logger.debug("Consuming messages from partition {} at address {}",
              partition.getPartitionId(), leaderPortModifiedAddress);

      MessageBatchReadResponse response = partitionClient.consumeMessage(
              leaderPortModifiedAddress,
              topic,
              partition.getPartitionId(),
              clientId,
              DEFAULT_MAX_MESSAGES
      );

      long offset = response.getOffset();
      List<String> messages = response.getMessages();
      int numMessages = messages.size();

      logger.info("Consumed {} messages from topic: {}, partition: {}",
              numMessages, topic, partition.getPartitionId());
      logger.debug("Current offset: {}, committing new offset: {}", offset, offset + numMessages);

      partitionClient.commitOffset(
              leaderPortModifiedAddress,
              topic,
              partition.getPartitionId(),
              clientId,
              offset + numMessages
      );

      return messages;
    } catch (Exception e) {
      logger.error("Failed to consume messages from topic: {}, partition: {}",
              topic, partition.getPartitionId(), e);
      throw new RuntimeException("Failed to consume messages", e);
    }
  }

  private String getPortModifiedAddress(String address) {
    logger.debug("Converting broker address: {}", address);
    String index = address.split("broker")[1];
    String modifiedAddress = brokerPeerIds.get(Integer.parseInt(String.valueOf(index.charAt(0))) - 1);
    logger.debug("Converted address {} to {}", address, modifiedAddress);
    return modifiedAddress;
  }
}