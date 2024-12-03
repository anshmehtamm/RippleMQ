package client;

public interface ProducerClient {

  /**
   * Produce a message to a topic.
   * @param topic
   * @param message
   */
  public void produce(String topic, String message);

  /**
   * Close the producer client.
   */
  public void close();
}
