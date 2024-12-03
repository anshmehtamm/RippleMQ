package metadata;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcClient;
import com.alipay.sofa.jraft.util.Endpoint;

import java.util.ArrayList;
import java.util.List;

import metadata.model.Topic;
import request.metadata.TopicsRequest;
import request.metadata.TopicsResponse;

public class MetadataClient {
  private final List<String> brokerAddresses;
  private final RpcClient rpcClient;
  public MetadataClient(List<String> brokerAddresses) {
    this.brokerAddresses = brokerAddresses;
    this.rpcClient = new BoltRpcClient(new com.alipay.remoting.rpc.RpcClient());
    this.rpcClient.init(new RpcOptions());

  }

  public List<Topic> fetchMetadata() {
    // pick a random broker to fetch metadata
    int retries = 3;
    while (retries-- > 0) {
      String brokerAddress = brokerAddresses.get((int) (Math.random() * brokerAddresses.size()));
      try {
        return fetchMetadataFromBroker(brokerAddress);
      } catch (Exception e) {
        System.out.println("Failed to fetch metadata from broker " + brokerAddress + ": " + e.getMessage());
        e.printStackTrace();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    }
    throw new RuntimeException("Failed to fetch metadata from brokers");
  }

  private List<Topic> fetchMetadataFromBroker(String brokerAddress) throws RemotingException, InterruptedException {
    Endpoint endpoint = PeerId.parsePeer(brokerAddress).getEndpoint();
    TopicsRequest request = new TopicsRequest();
    Object response = rpcClient.invokeSync(endpoint, request, 3000);

    if (response instanceof List<?>) {
      List<?> responseList = (List<?>) response;
      List<Topic> topicList = new ArrayList<>();
      for (Object obj : responseList) {
        if (!(obj instanceof Topic)) {
          throw new RuntimeException("Invalid response type");
        }else{
          topicList.add((Topic) obj);
        }
      }
      return topicList;
    }else if (response instanceof String) {
      throw new RuntimeException((String) response);
    }else{
      throw new RuntimeException("Invalid response type");
    }
  }

  public RpcClient getRpcClient() {
    return rpcClient;
  }






}
