package partition.selector;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import request.partition.*;

public class PartitionClient {
  private static final Logger logger = LoggerFactory.getLogger(PartitionClient.class);
  private final RpcClient rpcClient;

  public PartitionClient(RpcClient rpcClient) {
    this.rpcClient = rpcClient;
    logger.debug("Initialized PartitionClient with provided RpcClient");
  }

  public PartitionClient() {
    this.rpcClient = new BoltRpcClient(new com.alipay.remoting.rpc.RpcClient());
    this.rpcClient.init(new RpcOptions());
    logger.debug("Initialized PartitionClient with new RpcClient");
  }

  public void sendMessage(String brokerAddress, String topic, int partitionId, String message)
          throws RuntimeException {
    if (brokerAddress == null) {
      logger.error("Broker address cannot be null");
      throw new IllegalArgumentException("Invalid arguments");
    }

    Endpoint endpoint = PeerId.parsePeer(brokerAddress).getEndpoint();
    MessageAppendRequest request = new MessageAppendRequest(new ArrayList<>(List.of(message)),
            topic, partitionId);

    logger.debug("Sending message to topic: {}, partition: {} at broker: {}", topic, partitionId, brokerAddress);

    try {
      Object response = rpcClient.invokeSync(endpoint, request, 3000);
      if (response instanceof MessageAppendResponse) {
        MessageAppendResponse messageAppendResponse = (MessageAppendResponse) response;
        if (messageAppendResponse.isSuccess()) {
          logger.info("Successfully sent message to topic: {}, partition: {}", topic, partitionId);
        } else {
          logger.error("Failed to send message: {}", messageAppendResponse.getErrorMsg());
          throw new RuntimeException("Failed to send message: " + messageAppendResponse.getErrorMsg());
        }
      }
    } catch (Exception e) {
      logger.error("Exception while sending message to topic: {}, partition: {}", topic, partitionId, e);
      throw new RuntimeException("Failed to send message", e);
    }
  }

  public void close() {
    logger.info("Shutting down PartitionClient");
    rpcClient.shutdown();
  }

  public MessageBatchReadResponse consumeMessage(String brokerAddress,
                                                 String topic, int partitionId, String clientId, int maxMessages) {
    if (brokerAddress == null) {
      logger.error("Broker address cannot be null");
      throw new IllegalArgumentException("Invalid arguments");
    }

    logger.debug("Consuming messages for client: {} from topic: {}, partition: {}, max messages: {}",
            clientId, topic, partitionId, maxMessages);

    Endpoint endpoint = PeerId.parsePeer(brokerAddress).getEndpoint();
    MessageBatchReadRequest request = new MessageBatchReadRequest(clientId, maxMessages, topic, partitionId);

    try {
      Object response = rpcClient.invokeSync(endpoint, request, 3000);
      if (response instanceof MessageBatchReadResponse) {
        MessageBatchReadResponse batchResponse = (MessageBatchReadResponse) response;
        logger.debug("Successfully read {} messages for client: {} from topic: {}, partition: {}",
                batchResponse.getMessages().size(), clientId, topic, partitionId);
        return batchResponse;
      } else {
        String errorMsg = response instanceof String ? response.toString() : "Unknown error";
        logger.error("Failed to read messages: {}", errorMsg);
        throw new RuntimeException("Failed to read message: " + errorMsg);
      }
    } catch (Exception e) {
      logger.error("Exception while reading messages for client: {} from topic: {}, partition: {}",
              clientId, topic, partitionId, e);
      throw new RuntimeException("Failed to read message", e);
    }
  }

  public void commitOffset(String brokerAddress, String topic, int partitionId, String clientId, long offset) {
    if (brokerAddress == null) {
      logger.error("Broker address cannot be null");
      throw new IllegalArgumentException("Invalid arguments");
    }

    logger.debug("Committing offset {} for client: {} on topic: {}, partition: {}",
            offset, clientId, topic, partitionId);

    Endpoint endpoint = PeerId.parsePeer(brokerAddress).getEndpoint();
    ConsumerOffsetUpdateRequest request = new ConsumerOffsetUpdateRequest(clientId, offset, topic, partitionId);

    try {
      Object response = rpcClient.invokeSync(endpoint, request, 3000);
      if (response instanceof String) {
        logger.error("Failed to commit offset: {}", response);
        throw new RuntimeException("Failed to commit offset: " + response);
      }
      if (response instanceof ConsumerOffsetUpdateResponse) {
        ConsumerOffsetUpdateResponse offsetResponse = (ConsumerOffsetUpdateResponse) response;
        if (!offsetResponse.isSuccess()) {
          logger.error("Failed to commit offset: {}", offsetResponse.getErrorMsg());
          throw new RuntimeException("Failed to commit offset: " + offsetResponse.getErrorMsg());
        } else {
          logger.info("Successfully committed offset {} for client: {} on topic: {}, partition: {}",
                  offset, clientId, topic, partitionId);
        }
      }
    } catch (Exception e) {
      logger.error("Exception while committing offset for client: {} on topic: {}, partition: {}",
              clientId, topic, partitionId, e);
      throw new RuntimeException("Failed to commit offset", e);
    }
  }
}