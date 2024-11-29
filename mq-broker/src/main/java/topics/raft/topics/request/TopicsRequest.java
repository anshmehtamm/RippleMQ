package topics.raft.topics.request;

import java.io.Serializable;
import java.util.List;
import topics.Topic;

/**
 * TopicsRequest represents a request to either read or write the entire list of topics.
 */
public class TopicsRequest implements Serializable {

  private static final long serialVersionUID = 1L;

  private boolean isWrite; // true for write, false for read
  private List<Topic> topics; // Used only for write operations

  /**
   * Default constructor for read operations.
   */
  public TopicsRequest() {
    this.isWrite = false;
    this.topics = null;
  }

  /**
   * Constructor for write operations.
   *
   * @param topics The new list of topics to write
   */
  public TopicsRequest(List<Topic> topics) {
    this.isWrite = true;
    this.topics = topics;
  }

  /**
   * Constructor with isWrite flag.
   *
   * @param isWrite Indicates if the request is a write operation
   * @param topics  The new list of topics (used only if isWrite is true)
   */
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
