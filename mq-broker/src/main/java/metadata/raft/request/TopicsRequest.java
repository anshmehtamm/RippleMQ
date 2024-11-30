package metadata.raft.request;

import java.io.Serializable;
import java.util.List;

import partition.Topic;

/**
 * TopicsRequest represents a request to either read or write the entire list of topics.
 */
public class TopicsRequest implements Serializable {

  private static final long serialVersionUID = 1L;

  private boolean isWrite; // true for write, false for read
  private List<Topic> topics; // Used only for write operations

  // Constructors

  public TopicsRequest() {
    this.isWrite = false;
    this.topics = null;
  }

  public TopicsRequest(List<Topic> topics) {
    this.isWrite = true;
    this.topics = topics;
  }

  public TopicsRequest(boolean isWrite, List<Topic> topics) {
    this.isWrite = isWrite;
    this.topics = topics;
  }

  // Getters and Setters

  public boolean isWrite() {
    return isWrite;
  }

  public void setWrite(boolean write) {
    isWrite = write;
  }

  public List<Topic> getTopics() {
    return topics;
  }

  public void setTopics(List<Topic> topics) {
    this.topics = topics;
  }

  @Override
  public String toString() {
    return "TopicsRequest{isWrite=" + isWrite + ", topics=" + topics + "}";
  }
}
