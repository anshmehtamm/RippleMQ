package request.partition;

import java.util.List;

/**
 * MessageAppendRequest represents a request to append messages to the partition.
 */
public class MessageAppendRequest implements PartitionRequest {

  private static final long serialVersionUID = 1L;

  private List<String> messages;

  public MessageAppendRequest() {
  }

  public MessageAppendRequest(List<String> messages) {
    this.messages = messages;
  }

  // Getters and Setters
  public List<String> getMessages() {
    return messages;
  }

  public void setMessages(List<String> messages) {
    this.messages = messages;
  }
}
