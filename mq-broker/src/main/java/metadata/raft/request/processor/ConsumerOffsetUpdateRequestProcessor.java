package metadata.raft.request.processor;

import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import metadata.PartitionManager;
import metadata.raft.PartitionRaftServer;
import metadata.raft.request.PartitionClosure;
import request.partition.ConsumerOffsetUpdateRequest;
import request.partition.ConsumerOffsetUpdateResponse;
import request.partition.MessageAppendRequest;

public class ConsumerOffsetUpdateRequestProcessor implements RpcProcessor<ConsumerOffsetUpdateRequest> {

  private PartitionManager partitionManager;

  public ConsumerOffsetUpdateRequestProcessor() {
  }


  @Override
  public void handleRequest(RpcContext rpcCtx, ConsumerOffsetUpdateRequest request) {
    PartitionRaftServer partitionRaftServer = partitionManager.getPartitionRaftServer(request.getGroupId());
    if (!partitionRaftServer.getNode().isLeader()) {
      // throw error
      rpcCtx.sendResponse("Not leader");
    }
    handleConsumerOffsetUpdateRequest(rpcCtx, request, partitionRaftServer);
  }

  private void handleConsumerOffsetUpdateRequest(RpcContext rpcCtx, ConsumerOffsetUpdateRequest request, PartitionRaftServer partitionRaftServer) {
    // Create a closure to handle the response after operation
    PartitionClosure done = new PartitionClosure(partitionRaftServer, request, status -> {
      ConsumerOffsetUpdateResponse response = new ConsumerOffsetUpdateResponse();
      if (status.isOk()) {
        response.setSuccess(true);
      } else {
        response.setSuccess(false);
        response.setErrorMsg(status.getErrorMsg());
      }
      rpcCtx.sendResponse(response);
    });

    try {
      // Serialize the request
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(request);
      oos.flush();
      byte[] data = baos.toByteArray();

      // Apply the request to the Raft node
      partitionRaftServer.getNode().apply(new Task(ByteBuffer.wrap(data), done));
    } catch (IOException e) {
      e.printStackTrace();
      ConsumerOffsetUpdateResponse response = new ConsumerOffsetUpdateResponse();
      response.setSuccess(false);
      response.setErrorMsg("Serialization failed: " + e.getMessage());
      rpcCtx.sendResponse(response);
    }
  }

  @Override
  public String interest() {
    return ConsumerOffsetUpdateRequest.class.getName();
  }

  @Override
  public Executor executor() {
    return null; // Use default executor
  }

  public void setPartitionManager(PartitionManager partitionManager) {
    this.partitionManager = partitionManager;
  }
}
