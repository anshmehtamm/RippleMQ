package request.partition;

import java.util.List;

/**
 * MessageAppendRequest represents a request to append messages to the partition.
 */
public class MessageAppendRequest implements PartitionRequest {

  private static final long serialVersionUID = 1L;

  private List<String> messages;
  private String topicName;
  private int partitionId;

  public MessageAppendRequest() {
  }

  public MessageAppendRequest(List<String> messages, String topicName, int partitionId) {
    this.messages = messages;
    this.topicName = topicName;
    this.partitionId = partitionId;
  }

  // Getters and Setters
  public List<String> getMessages() {
    return messages;
  }

  public void setMessages(List<String> messages) {
    this.messages = messages;
  }


  @Override
  public String getGroupId() {
    return topicName+"-"+partitionId;
  }
}
