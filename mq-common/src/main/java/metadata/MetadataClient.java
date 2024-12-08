package metadata;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import metadata.model.Topic;
import request.metadata.TopicsRequest;
import request.metadata.TopicsResponse;

public class MetadataClient {
  private static final Logger logger = LoggerFactory.getLogger(MetadataClient.class);

  private final List<String> brokerAddresses;
  private final RpcClient rpcClient;

  public MetadataClient(List<String> brokerAddresses) {
    this.brokerAddresses = brokerAddresses;
    this.rpcClient = new BoltRpcClient(new com.alipay.remoting.rpc.RpcClient());
    this.rpcClient.init(new RpcOptions());

    logger.info("Initialized MetadataClient with {} broker addresses", brokerAddresses.size());
    logger.debug("Broker addresses: {}", brokerAddresses);
  }

  public List<Topic> fetchMetadata() {
    logger.debug("Attempting to fetch metadata from brokers");
    int retries = 3;

    while (retries-- > 0) {
      String brokerAddress = brokerAddresses.get(
              (int) (Math.random() * brokerAddresses.size()));

      try {
        logger.debug("Attempting to fetch metadata from broker: {}", brokerAddress);
        List<Topic> topics = fetchMetadataFromBroker(brokerAddress);
        logger.info("Successfully fetched metadata from broker: {}", brokerAddress);
        return topics;
      } catch (Exception e) {
        logger.warn("Failed to fetch metadata from broker: {}, {} attempts remaining",
                brokerAddress, retries, e);

        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          logger.warn("Sleep interrupted while waiting to retry", ie);
        }
      }
    }

    logger.error("Failed to fetch metadata after all retry attempts");
    throw new RuntimeException("Failed to fetch metadata from brokers");
  }

  private List<Topic> fetchMetadataFromBroker(String brokerAddress)
          throws RemotingException, InterruptedException {
    Endpoint endpoint = PeerId.parsePeer(brokerAddress).getEndpoint();
    TopicsRequest request = new TopicsRequest();

    logger.debug("Sending topics request to endpoint: {}", endpoint);
    Object response = rpcClient.invokeSync(endpoint, request, 3000);

    if (response instanceof List<?>) {
      List<?> responseList = (List<?>) response;
      List<Topic> topicList = new ArrayList<>();

      for (Object obj : responseList) {
        if (!(obj instanceof Topic)) {
          logger.error("Invalid response type received: {}", obj.getClass());
          throw new RuntimeException("Invalid response type");
        } else {
          topicList.add((Topic) obj);
        }
      }

      logger.debug("Received {} topics from broker", topicList.size());
      return topicList;
    } else if (response instanceof String) {
      logger.error("Error response received from broker: {}", response);
      throw new RuntimeException((String) response);
    } else {
      logger.error("Unexpected response type: {}",
              response != null ? response.getClass() : "null");
      throw new RuntimeException("Invalid response type");
    }
  }

  public RpcClient getRpcClient() {
    return rpcClient;
  }
}