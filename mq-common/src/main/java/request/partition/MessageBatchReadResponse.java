package request.partition;

import java.io.Serializable;
import java.util.List;

/**
 * MessageBatchReadResponse represents the response to a MessageBatchReadRequest.
 */
public class MessageBatchReadResponse implements Serializable {

  private static final long serialVersionUID = 1L;

  private List<String> messages;
  private long offset; // The current offset of the consumer
  private boolean success = true;
  private int errorCode;
  private String errorMsg;

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
