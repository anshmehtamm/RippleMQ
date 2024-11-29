package broker;

import java.io.Serializable;

/**
 * BrokerInfo represents the information of a broker.
 */
public class BrokerInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private String id;
  private String hostname;
  private int port;

  /**
   * Constructor with parameters.
   *
   * @param id       The unique identifier of the broker
   * @param hostname The hostname of the broker
   * @param port     The port number of the broker
   */
  public BrokerInfo(String id, String hostname, int port) {
    this.id = id;
    this.hostname = hostname;
    this.port = port;
  }

  /**
   * Default constructor for loading from YAML or other serialization mechanisms.
   */
  public BrokerInfo() {
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

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof BrokerInfo)) return false;
    BrokerInfo other = (BrokerInfo) obj;
    return this.id.equals(other.id) &&
            this.hostname.equals(other.hostname) &&
            this.port == other.port;
  }

  @Override
  public int hashCode() {
    return id.hashCode() + hostname.hashCode() + port;
  }
}
