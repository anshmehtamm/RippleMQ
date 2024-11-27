package messaging;

import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Ripplemq <T, M> implements ProducerAPI<T, M> {

  private int no_of_brokers;

  private int replication_servers;

  private Boolean ack_req = null;

  private int currentBrokerIndex = 0; // To keep track of the round-robin broker selection

  private List<String> brokers; // List of broker hostnames

  private int brokerPort = 8080; //temp

  private Map<String, Socket> brokerConnections; // Stores active broker connections

  public Ripplemq(int n_brokers, int repl_servers, Boolean ack) {
    this.no_of_brokers = n_brokers;
    this.replication_servers = repl_servers;
    this.ack_req = ack;
    this.brokers = new ArrayList<>(); //read from metadata
    this.brokerConnections = new HashMap<>();
  }

  /**
   * Produces a new message to the specified topic.
   *
   * @param topic   The topic to which the message will be sent.
   * @param message The message object to be sent.
   */
  @Override
  public void produce(T topic, M message) {

    // TODO: Implement message production logic
    // 1. Validate inputs (topic and message)
    // 2. Select a broker (e.g., using round-robin)
    // 3. Establish a connection to the broker
    // 4. Send the message to the broker
    // 5. Optionally, wait for acknowledgments (if ack_req is true)
    // 6. Handle any errors or retries

    try {
      // Get the current broker using round-robin
      String selectedBroker = brokers.get(currentBrokerIndex);

      // Connect to the broker
      if (!brokerConnections.containsKey(selectedBroker)) {
        connectToBroker(selectedBroker);
      }

      // Simulate sending the message
      Socket brokerSocket = brokerConnections.get(selectedBroker);
      OutputStream outputStream = brokerSocket.getOutputStream();
      String msgToSend = "Topic: " + topic + ", Message: " + message + "\n";
      outputStream.write(msgToSend.getBytes());
      outputStream.flush();
      System.out.println("Message sent to topic: " + topic + " on broker: " + selectedBroker);

      if (ack_req) {
        System.out.println("Acknowledgment requested (not implemented in simulation).");
      }

      // Move to the next broker in the round-robin
      currentBrokerIndex = (currentBrokerIndex + 1) % no_of_brokers;

    } catch (Exception e) {
      System.err.println("Error producing message: " + e.getMessage());
    }

  }

  /**
   * Connects to the list of brokers to which the producer will connect.
   * Returns - The Acknowledgement of connection with the brokers
   */
  @Override
  public int connect() {
    // TODO: Implement broker connection logic
    // 1. Iterate over the list of brokers
    // 2. Establish connections to each broker
    // 3. Return an acknowledgment code (e.g., success or failure count)

    int successCount = 0;
    System.out.println("Connecting to all brokers...");
    for (String broker : brokers) {
      if (connectToBroker(broker)) {
        successCount++;
      }
    }
    System.out.println("Successfully connected to " + successCount + " out of " + no_of_brokers + " brokers.");
    return successCount;

  }

  /**
   * Closes all open connections and releases resources.
   */
  @Override
  public void disconnect() {

    // TODO: Implement broker disconnection logic
    // 1. Iterate over all active connections
    // 2. Close each connection and release resources
    // 3. Handle any exceptions during cleanup

    System.out.println("Disconnecting from all brokers...");
    for (String broker : brokers) {
      disconnectFromBroker(broker);
    }
    System.out.println("All brokers disconnected successfully.");

  }

  // Establishes a TCP connection to the broker
  private boolean connectToBroker(String broker) {
    try {
      System.out.println("Connecting to broker: " + broker + " on port: " + brokerPort);
      Socket socket = new Socket("localhost", brokerPort); // Replace "localhost" with the broker hostname/IP
      brokerConnections.put(broker, socket);
      return true;
    } catch (Exception e) {
      System.err.println("Failed to connect to broker: " + broker + ". Error: " + e.getMessage());
      return false;
    }
  }

  // Closes the TCP connection to the broker
  private void disconnectFromBroker(String broker) {
    try {
      if (brokerConnections.containsKey(broker)) {
        Socket socket = brokerConnections.get(broker);
        socket.close();
        brokerConnections.remove(broker);
        System.out.println("Disconnected from broker: " + broker);
      }
    } catch (Exception e) {
      System.err.println("Error disconnecting from broker: " + broker + ". Error: " + e.getMessage());
    }
  }

}
