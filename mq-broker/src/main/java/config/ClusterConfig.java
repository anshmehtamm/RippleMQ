package config;

import java.io.Serializable;
import java.util.List;

import metadata.model.Topic;

/**
 * ClusterConfig holds the configuration of the cluster, including brokers and topics.
 */
public class ClusterConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private List<BrokerConfig> brokers;
  private List<Topic> topics;
  private String configPath; // Path to the configuration file (optional)
  private String enableAutoCommit; // Enable auto commit for consumers

  // Getters and Setters

  public List<BrokerConfig> getBrokers() {
    return brokers;
  }

  public void setBrokers(List<BrokerConfig> brokers) {
    this.brokers = brokers;
  }

  public List<Topic> getTopics() {
    return topics;
  }

  public void setTopics(List<Topic> topics) {
    this.topics = topics;
  }

  public String getConfigPath() {
    return configPath;
  }

  public void setConfigPath(String configPath) {
    this.configPath = configPath;
  }


  /**
   * Retrieves the BrokerConfig for the given broker ID.
   *
   * @param id The broker ID
   * @return The corresponding BrokerConfig, or null if not found
   */
  public BrokerConfig getBrokerConfig(String id) {
    for (BrokerConfig broker : brokers) {
      if (broker.getId().equals(id)) {
        return broker;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "ClusterConfig{brokers=" + brokers + ", topics=" + topics + "}";
  }

  /**
   * BrokerConfig represents the configuration information of a broker.
   */
  public static class BrokerConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String hostname;
    private int port;

    // Constructors

    public BrokerConfig() {
    }

    public BrokerConfig(String id, String hostname, int port) {
      this.id = id;
      this.hostname = hostname;
      this.port = port;
    }

    // Getters and Setters

    public String getId() {
      return id;
    }

    public String getHostname() {
      return hostname;
    }

    public int getPort() {
      return port;
    }

    public void setId(String id) {
      this.id = id;
    }

    public void setHostname(String hostname) {
      this.hostname = hostname;
    }

    public void setPort(int port) {
      this.port = port;
    }

    @Override
    public String toString() {
      return id + "@" + hostname + ":" + port;
    }
  }
}
