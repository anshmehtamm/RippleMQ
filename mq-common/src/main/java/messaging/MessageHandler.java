package messaging;


public interface MessageHandler {

  /**
   * Handles a message. Each handler should be able to handle messages of different types.
   * @param message
   * @param fromId
   */
  void handleMessage(Message message, int fromId);
}
