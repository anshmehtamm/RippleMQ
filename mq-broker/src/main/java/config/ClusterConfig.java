package config;

import java.io.Serializable;
import java.util.List;

import broker.BrokerInfo;
import topics.Topic;

/**
 * ClusterConfig holds the configuration of the cluster, including brokers and topics.
 */
public class ClusterConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private List<BrokerInfo> brokers;
  private List<Topic> topics;
  private String configPath; // Path to the configuration file (optional)

  /**
   * Default constructor.
   */
  public ClusterConfig() {
  }

  // Getters and Setters

  public List<BrokerInfo> getBrokers() {
    return brokers;
  }

  public void setBrokers(List<BrokerInfo> brokers) {
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
   * Retrieves the BrokerInfo for the given broker ID.
   *
   * @param id The broker ID
   * @return The corresponding BrokerInfo, or null if not found
   */
  public BrokerInfo getSelf(String id) {
    for (BrokerInfo broker : brokers) {
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
}
