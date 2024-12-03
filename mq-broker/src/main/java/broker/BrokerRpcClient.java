package broker;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcClient;
import com.alipay.sofa.jraft.util.Endpoint;

import request.partition.PartitionLeaderUpdateRequest;

public class BrokerRpcClient {

  private final RpcClient rpcClient;
  private static BrokerRpcClient instance;


  private BrokerRpcClient(PeerId peerId) {
    this.rpcClient = new BoltRpcClient(new com.alipay.remoting.rpc.RpcClient());
    this.rpcClient.init(new RpcOptions());
  }

  public void updatePartitionLeader(Endpoint leader, String groupId, String newLeaderAddress)
  throws RuntimeException {
    PartitionLeaderUpdateRequest request = new PartitionLeaderUpdateRequest(groupId, newLeaderAddress);

    try {
      Object response = rpcClient.invokeSync(leader, request, 5000);
      String responseString = (String) response;
      if (!responseString.equals("Success")) {
        throw new RuntimeException("Failed to update partition leader: " + responseString);
      }
    } catch (RemotingException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static BrokerRpcClient getInstance(PeerId peerId){
    if (instance == null) {
      instance = new BrokerRpcClient(peerId);
      return instance;
    }else{
      return instance;
    }
  }
}
