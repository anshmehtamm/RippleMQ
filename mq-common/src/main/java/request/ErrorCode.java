package request;

/**
 * Simple error codes returned by brokers to clients.
 */
public enum ErrorCode {
  SUCCESS(0, "Success"),
  NOT_LEADER(1, "Not leader"),
  INTERNAL_ERROR(2, "Internal error");

  private final int code;
  private final String defaultMessage;

  ErrorCode(int code, String defaultMessage) {
    this.code = code;
    this.defaultMessage = defaultMessage;
  }

  public int getCode() {
    return code;
  }

  public String getDefaultMessage() {
    return defaultMessage;
  }
}
