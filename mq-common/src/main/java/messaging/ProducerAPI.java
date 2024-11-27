package messaging;

/**
 * Interface for producers to send messages to brokers using round-robin distribution.
 */
public interface ProducerAPI <T, M> {

  /**
   * Produces a new message to the specified topic.
   *
   * @param topic The topic to which the message will be sent.
   * @param message The message object to be sent.
   */
  void produce(T topic, M message);

  /**
   * Connects to the list of brokers to which the producer will connect.
   * Returns - The Acknowledgement of connection with the brokers
   */
  int connect();

  /**
   * Closes all open connections and releases resources.
   */
  void disconnect();



}
