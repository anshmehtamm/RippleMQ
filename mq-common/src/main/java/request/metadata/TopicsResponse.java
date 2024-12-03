package request.metadata;

import java.io.Serializable;
import java.util.List;

import metadata.model.Topic;

/**
 * TopicsResponse represents the server's response to a TopicsRequest.
 */
public class TopicsResponse implements Serializable {

  private static final long serialVersionUID = 1L;

  private boolean success;
  private List<Topic> topics; // Used for read operations
  private String errorMsg;

  // Getters and Setters

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public List<Topic> getTopics() {
    return topics;
  }

  public void setTopics(List<Topic> topics) {
    this.topics = topics;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg(String errorMsg) {
    this.errorMsg = errorMsg;
  }

  @Override
  public String toString() {
    return "TopicsResponse{success=" + success + ", topics=" + topics + ", errorMsg='" + errorMsg + "'}";
  }
}
