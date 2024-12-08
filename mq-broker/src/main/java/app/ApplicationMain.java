package app;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import broker.BrokerServer;
import config.ClusterConfigManager;

public class ApplicationMain {
  private static final Logger logger = LoggerFactory.getLogger(ApplicationMain.class);

  public static void main(String[] args) throws IOException {
    setSystemProperties();

    if (args.length < 1) {
      logger.error("Missing required argument: brokerId");
      System.exit(1);
    }

    String brokerId = args[1];
    logger.info("Starting broker with ID: {}", brokerId);

    // Load cluster configuration
    ClusterConfigManager clusterConfigManager = ClusterConfigManager.getInstance();
    try {
      clusterConfigManager.loadClusterConfig();
      logger.info("Successfully loaded cluster configuration");
    } catch (Exception e) {
      logger.error("Failed to load cluster configuration", e);
      System.exit(1);
    }

    // Instantiate BrokerServer
    BrokerServer brokerServer = new BrokerServer(clusterConfigManager.getClusterConfig(), brokerId);

    // Start BrokerServer
    try {
      brokerServer.start();
      logger.info("Broker server started successfully");
    } catch (Exception e) {
      logger.error("Failed to start broker server", e);
      System.exit(1);
    }

    // Add shutdown hook to gracefully stop the server
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        logger.info("Shutting down broker server...");
        brokerServer.stop();
        logger.info("Broker server stopped successfully");
      } catch (Exception e) {
        logger.error("Error during broker server shutdown", e);
      }
    }));
  }

  private static void setSystemProperties() {
    // You can add any system property settings here if needed
    logger.debug("Setting system properties");
  }
}