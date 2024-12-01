package partition.raft.request;

import java.io.Serializable;
import java.util.List;

/**
 * MessageBatchReadResponse represents the response to a MessageBatchReadRequest.
 */
public class MessageBatchReadResponse implements Serializable {

  private static final long serialVersionUID = 1L;

  private List<String> messages;
  private long offset; // The current offset of the consumer

  public MessageBatchReadResponse() {
  }

  // Getters and Setters
  public List<String> getMessages() {
    return messages;
  }

  public void setMessages(List<String> messages) {
    this.messages = messages;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }
}
