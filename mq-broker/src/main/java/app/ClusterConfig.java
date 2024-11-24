package app;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ClusterConfig {
  private List<Broker> brokers;
  private List<Topic> topics;
  private int port;

  private Broker self;

  public ClusterConfig() {}

  // Getters and Setters
  public List<Broker> getBrokers() {
    return brokers;
  }

  public void setBrokers(List<Broker> brokers) {
    this.brokers = brokers;
    // identify self
    for (Broker broker : brokers) {
      // get hostname of the current machine
      String hostname = new InetSocketAddress(0).getAddress().getHostAddress();
      System.out.println("Hostname: " + hostname);
      if (broker.getHost().equals(hostname)) {
        self = broker;
        //this.brokers.remove(broker);
        break;
      }
    }
  }

  public List<Topic> getTopics() {
    return topics;
  }

  public Broker getSelf() {
    return self;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setTopics(List<Topic> topics) {

    // Validate topics
    if (topics.isEmpty()) {
      throw new IllegalArgumentException("At least one topic must be defined");
    }
    if (topics.stream().anyMatch(topic -> topic.getReplication_factor() > brokers.size())) {
      throw new IllegalArgumentException("Replication factor cannot be greater than the number of brokers");
    }

    this.topics = topics;
  }

  public static ClusterConfig loadConfig(String filePath) throws IOException {
    Yaml yaml = new Yaml(new Constructor(ClusterConfig.class));
    try (InputStream in = Files.newInputStream(Paths.get(filePath))) {
      return yaml.load(in);
    }
  }
}
