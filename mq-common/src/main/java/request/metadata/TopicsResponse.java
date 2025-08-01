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
  private int errorCode;
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

  public int getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  @Override
  public String toString() {
    return "TopicsResponse{success=" + success + ", topics=" + topics +
            ", errorCode=" + errorCode + ", errorMsg='" + errorMsg + "'}";
  }
}
