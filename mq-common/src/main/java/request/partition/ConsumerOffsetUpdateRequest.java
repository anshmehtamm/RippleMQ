package request.partition;

/**
 * ConsumerOffsetUpdateRequest represents a request to update the consumer's offset after acknowledgment.
 */
public class ConsumerOffsetUpdateRequest implements PartitionRequest {

  private static final long serialVersionUID = 1L;

  private String consumerId;
  private long offset; // New offset after acknowledgment
  private String topicName;
  private int partitionId;

  public ConsumerOffsetUpdateRequest() {
  }

  public ConsumerOffsetUpdateRequest(String consumerId, long offset, String topicName, int partitionId) {
    this.consumerId = consumerId;
    this.offset = offset;
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

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public String getGroupId() {
    return topicName+"-"+partitionId;
  }
}
