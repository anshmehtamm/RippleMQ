package partition.selector;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcClient;
import com.alipay.sofa.jraft.util.Endpoint;

import java.util.ArrayList;
import java.util.List;

import request.partition.ConsumerOffsetUpdateRequest;
import request.partition.ConsumerOffsetUpdateResponse;
import request.partition.MessageAppendRequest;
import request.partition.MessageAppendResponse;
import request.partition.MessageBatchReadRequest;
import request.partition.MessageBatchReadResponse;
import request.partition.PartitionRequest;

public class PartitionClient {

  private final RpcClient rpcClient;


  public PartitionClient(RpcClient rpcClient) {
    this.rpcClient = rpcClient;
  }

  public PartitionClient() {
    this.rpcClient = new BoltRpcClient(new com.alipay.remoting.rpc.RpcClient());
    this.rpcClient.init(new RpcOptions());
  }



  public void sendMessage
          (String brokerAddress, String topic, int partitionId, String message)
    throws RuntimeException{
    if (brokerAddress== null) {
      throw new IllegalArgumentException("Invalid arguments");
    }
    Endpoint endpoint = PeerId.parsePeer(brokerAddress).getEndpoint();

    MessageAppendRequest request = new MessageAppendRequest(new ArrayList<>(List.of(message)),
            topic, partitionId);


    try {
      Object response = rpcClient.invokeSync(endpoint, request, 3000);
      if(response instanceof MessageAppendResponse){
        MessageAppendResponse messageAppendResponse = (MessageAppendResponse) response;
        if(messageAppendResponse.isSuccess()){
          System.out.println("Message sent successfully");
        }else{
          System.out.println("Failed to send message: "+messageAppendResponse.getErrorMsg());
          throw new RuntimeException("Failed to send message");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to send message");
    }

  }

  public void close(){
    rpcClient.shutdown();
  }


  public MessageBatchReadResponse consumeMessage(String brokerAddress,
                             String topic, int partitionId, String clientId, int maxMessages) {

    if (brokerAddress== null) {
      throw new IllegalArgumentException("Invalid arguments");
    }
    Endpoint endpoint = PeerId.parsePeer(brokerAddress).getEndpoint();

    MessageBatchReadRequest request = new MessageBatchReadRequest(clientId, maxMessages, topic,
            partitionId);

    try {
      Object response = rpcClient.invokeSync(endpoint, request, 3000);
      if (response instanceof MessageBatchReadResponse){
        return (MessageBatchReadResponse) response;
      }else{
        if (response instanceof String){
          System.out.println("Failed to read message: "+response);
          throw new RuntimeException("Failed to read message: "+response);
        }
        throw new RuntimeException("Failed to read message");
      }
    }catch (Exception e){
      e.printStackTrace();
      throw new RuntimeException("Failed to read message");
    }

  }

  public void commitOffset(String brokerAddress, String topic, int partitionId, String clientId, long l) {

    if (brokerAddress== null) {
      throw new IllegalArgumentException("Invalid arguments");
    }

    Endpoint endpoint = PeerId.parsePeer(brokerAddress).getEndpoint();

    ConsumerOffsetUpdateRequest request = new ConsumerOffsetUpdateRequest(clientId, l, topic, partitionId);

    try {
      Object response = rpcClient.invokeSync(endpoint, request, 3000);
      if (response instanceof String) {
        System.err.println("Failed to commit offset: " + response);
        throw new RuntimeException("Failed to commit offset: " + response);
      }
      if (response instanceof ConsumerOffsetUpdateResponse) {
        if (!((ConsumerOffsetUpdateResponse) response).isSuccess()) {
          System.err.println("Failed to commit offset: " + ((ConsumerOffsetUpdateResponse) response).getErrorMsg());
          throw new RuntimeException("Failed to commit offset: " + ((ConsumerOffsetUpdateResponse) response).getErrorMsg());
        } else {
          System.err.println("Offset committed successfully");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to commit offset");
    }


  }
}
