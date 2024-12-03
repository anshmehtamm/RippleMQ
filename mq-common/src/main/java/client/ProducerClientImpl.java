package client;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import metadata.MetadataManager;
import metadata.model.PartitionAssignment;
import partition.selector.PartitionClient;
import partition.selector.PartitionSelector;
import partition.selector.RoundRobinSelector;

public class ProducerClientImpl implements ProducerClient {

  private final String clientId;
  private static final int DEFAULT_REFRESH_INTERVAL_MS = 10_000;
  private PartitionSelector partitionSelector;
  private MetadataManager metadataManager;
  private final ScheduledExecutorService metadataRefreshScheduler;
  private final PartitionClient partitionClient;
  private List<String> brokerPeerIds;


  public ProducerClientImpl(String clientId, List<String> brokerPeerIds, int metadataRefreshIntervalMs) {
    this.clientId = clientId;
    partitionSelector = new RoundRobinSelector();
    this.brokerPeerIds = brokerPeerIds;
    metadataManager = new MetadataManager(brokerPeerIds);
    partitionClient = new PartitionClient(metadataManager.getRpcClient());
    metadataRefreshScheduler = Executors.newSingleThreadScheduledExecutor();
    metadataManager.refreshMetadata();
    periodicMetadataRefresh(metadataRefreshIntervalMs);
  }

  public ProducerClientImpl(String clientId, List<String> brokerPeerIds) {
    this(clientId, brokerPeerIds, DEFAULT_REFRESH_INTERVAL_MS);
  }


  private void periodicMetadataRefresh(int metadataRefreshIntervalMs) {
    this.metadataRefreshScheduler.scheduleAtFixedRate(() -> {
      try {
        metadataManager.refreshMetadata();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, 0, metadataRefreshIntervalMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void produce(String topic, String message) throws RuntimeException{
    List<PartitionAssignment> partitions = metadataManager.getPartitionsForTopic(topic);
    if (partitions == null) {
      System.out.println("Topic not found: " + topic);
      throw new RuntimeException("Topic not found: " + topic);
    }
    if (partitions.isEmpty()) {
      System.out.println("No partitions found for topic: " + topic);
      throw new RuntimeException("No partitions found for topic: " + topic);
    }
    PartitionAssignment partition = partitionSelector.selectPartition(topic, partitions);
    try {
      String leaderAddress = partition.getLeader();
      if (leaderAddress == null) {
        System.out.println("No leader found for partition: " + partition.getPartitionId());
        throw new RuntimeException("No leader found for partition: " + partition.getPartitionId());
      }
      String leaderPortModifiedAddress = getPortModifiedAddress(leaderAddress);
      partitionClient.sendMessage(leaderPortModifiedAddress, topic, partition.getPartitionId(), message);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to send message");
    }
   }

  @Override
  public void close() {
    metadataRefreshScheduler.shutdown();
    partitionClient.close();
  }

  /**
   * TODO: Change this logic
   * @param address
   * @return
   */
  private String getPortModifiedAddress(String address){
    System.err.println("Address: " + address);
    String index = address.split("broker")[1];
    return brokerPeerIds.get(Integer.parseInt(String.valueOf(index.charAt(0))) - 1);
  }
}
