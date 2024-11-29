package config;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * ClusterConfigManager manages the loading and accessing of the cluster configuration.
 */
public class ClusterConfigManager {

  private static ClusterConfigManager instance = null;

  private static final String DEFAULT_CONFIG_PATH = "mq-broker/config/cluster_config.yaml";

  private ClusterConfig clusterConfig;

  /**
   * Private constructor to enforce singleton pattern.
   */
  private ClusterConfigManager() {
  }

  /**
   * Retrieves the singleton instance of ClusterConfigManager.
   *
   * @return The singleton instance
   */
  public static synchronized ClusterConfigManager getInstance() {
    if (instance == null) {
      instance = new ClusterConfigManager();
    }
    return instance;
  }

  /**
   * Loads the cluster configuration from the specified path.
   *
   * @param path The path to the configuration YAML file
   * @return The loaded ClusterConfig
   * @throws IOException If an I/O error occurs
   */
  public ClusterConfig loadClusterConfig(String path) throws IOException {
    Yaml yaml = new Yaml(new Constructor(ClusterConfig.class));
    try (InputStream in = Files.newInputStream(Paths.get(path))) {
      this.clusterConfig = yaml.load(in);
    }
    return this.clusterConfig;
  }

  /**
   * Loads the cluster configuration from the default path.
   *
   * @return The loaded ClusterConfig
   * @throws IOException If an I/O error occurs
   */
  public ClusterConfig loadClusterConfig() throws IOException {
    return loadClusterConfig(DEFAULT_CONFIG_PATH);
  }

  /**
   * Retrieves the loaded ClusterConfig.
   *
   * @return The ClusterConfig
   * @throws IllegalStateException If the configuration has not been loaded yet
   */
  public ClusterConfig getClusterConfig() {
    if (this.clusterConfig == null) {
      throw new IllegalStateException("Cluster configuration not loaded");
    }
    return this.clusterConfig;
  }
}
