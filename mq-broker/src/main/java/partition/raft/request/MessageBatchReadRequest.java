package partition.raft.request;

/**
 * MessageBatchReadRequest represents a request from a consumer to read messages.
 */
public class MessageBatchReadRequest implements PartitionRequest {

  private static final long serialVersionUID = 1L;

  private String consumerId;
  private int maxMessages;

  public MessageBatchReadRequest() {
  }

  public MessageBatchReadRequest(String consumerId, int maxMessages) {
    this.consumerId = consumerId;
    this.maxMessages = maxMessages;
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
}
