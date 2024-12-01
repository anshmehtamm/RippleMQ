package partition.raft.request;

/**
 * ConsumerOffsetUpdateRequest represents a request to update the consumer's offset after acknowledgment.
 */
public class ConsumerOffsetUpdateRequest implements PartitionRequest {

  private static final long serialVersionUID = 1L;

  private String consumerId;
  private long offset; // New offset after acknowledgment

  public ConsumerOffsetUpdateRequest() {
  }

  public ConsumerOffsetUpdateRequest(String consumerId, long offset) {
    this.consumerId = consumerId;
    this.offset = offset;
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
}
