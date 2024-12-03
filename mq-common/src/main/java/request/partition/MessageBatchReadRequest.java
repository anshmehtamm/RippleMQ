package request.partition;

/**
 * MessageBatchReadRequest represents a request from a consumer to read messages.
 */
public class MessageBatchReadRequest implements PartitionRequest {

  private static final long serialVersionUID = 1L;

  private String consumerId;
  private int maxMessages;
  private String topicName;
  private int partitionId;

  public MessageBatchReadRequest() {
  }

  public MessageBatchReadRequest(String consumerId, int maxMessages, String topicName, int partitionId) {
    this.consumerId = consumerId;
    this.maxMessages = maxMessages;
    this.topicName = topicName;
    this.partitionId = partitionId;

  }

  // Getters and Setters
  public String getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(String consumerId) {
    this.consumerId = consumerId;
  }

  public int getMaxMessages() {
    return maxMessages;
  }

  public void setMaxMessages(int maxMessages) {
    this.maxMessages = maxMessages;
  }

  @Override
  public String getGroupId() {
    return topicName+"-"+partitionId;
  }
}
