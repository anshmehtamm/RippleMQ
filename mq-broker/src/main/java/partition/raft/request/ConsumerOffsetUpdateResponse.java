package partition.raft.request;

import java.io.Serializable;

/**
 * ConsumerOffsetUpdateResponse represents the response to a ConsumerOffsetUpdateRequest.
 */
public class ConsumerOffsetUpdateResponse implements Serializable {

  private static final long serialVersionUID = 1L;

  private boolean success;
  private String errorMsg;

  public ConsumerOffsetUpdateResponse() {
  }

  // Getters and Setters
  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg(String errorMsg) {
    this.errorMsg = errorMsg;
  }
}
