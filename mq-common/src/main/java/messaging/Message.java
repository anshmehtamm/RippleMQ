package messaging;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class Message {


  public enum MessageType {

    PROPOSE;

    public static Message createEmptyMessage(MessageType messageType) {
      switch (messageType) {
        default:
          return null;
      }
    }

  }

  private MessageType messageType;

  public Message(MessageType messageType) {
    this.messageType = messageType;
  }

  public MessageType getMessageType() {
    return messageType;
  }
  public abstract void serialize(DataOutputStream messageDos) throws IOException;
  public abstract void deserialize(DataInputStream messageDis) throws IOException;

}
