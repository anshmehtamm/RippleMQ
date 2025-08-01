package request.partition;

import java.io.Serializable;

/**
 * Response for PartitionLeaderUpdateRequest.
 */
public class PartitionLeaderUpdateResponse implements Serializable {

  private static final long serialVersionUID = 1L;

  private boolean success;
  private int errorCode;
  private String errorMsg;

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg(String errorMsg) {
    this.errorMsg = errorMsg;
  }
}
