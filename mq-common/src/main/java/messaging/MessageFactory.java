package messaging;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MessageFactory {

  public static void serializeMessage(Message message, DataOutputStream messageDos) throws IOException {
    messageDos.writeByte(message.getMessageType().ordinal());
    message.serialize(messageDos);
  }

  public static Message deserializeMessage(DataInputStream messageDis) throws IOException {
    int messageTypeOrdinal = messageDis.readByte();
    Message.MessageType messageType = Message.MessageType.values()[messageTypeOrdinal];
    Message message = Message.MessageType.createEmptyMessage(messageType);
    message.deserialize(messageDis);
    return message;
  }


}
