package messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import messaging.connection.TCPConnectionManager;

public class MessageSender {

  TCPConnectionManager connectionManager;
  private final Map<Message.MessageType, List<MessageHandler>> handlers = new ConcurrentHashMap<>();
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private final Map<Message.MessageType, Queue<IncomingMessage>> retryQueue = new ConcurrentHashMap<>();

  public MessageSender(int peerId, Map<Integer, String> peers) {
    connectionManager = new TCPConnectionManager(peerId, peers, this);
  }

  public void sendMessage(int toId, Message message){
    connectionManager.sendMessage(message, toId);
  }

  /**
   * Registers a handler for a message type.
   * @param messageType
   * @param handler the handler to register
   */
  public void registerMessageHandler(Message.MessageType messageType, MessageHandler handler) {
    handlers.computeIfAbsent(messageType, k -> new ArrayList<>()).add(handler);
    // dispatch any messages in retry queue
    Queue<IncomingMessage> messages = retryQueue.get(messageType);
    if (messages != null) {
      while (!messages.isEmpty()) {
        IncomingMessage message = messages.poll();
        handler.handleMessage(message.message, message.fromId);
      }
    }
  }

  /**
   * Dispatches a message to all handlers registered for its type.
   * @param message
   * @param fromId
   */
  public void dispatchMessage(Message message, int fromId) {
    List<MessageHandler> handlersForType = handlers.get(message.getMessageType());
    if (handlersForType != null) {
      for (MessageHandler handler : handlersForType) {
        // Dispatch each message to a new thread
        executorService.submit(() -> handler.handleMessage(message, fromId));
      }
    } else {
      // add to retry queue, once handler is registered, dispatch message
      retryQueue.computeIfAbsent(message.getMessageType(), k -> new LinkedBlockingQueue<>()).offer(new IncomingMessage(message, fromId));
      System.err.println("No handler registered for message type: " + message.getMessageType());
    }
  }

  private static class IncomingMessage{
    Message message;
    int fromId;
    public IncomingMessage(Message message, int fromId){
      this.message = message;
      this.fromId = fromId;
    }
  }

}
