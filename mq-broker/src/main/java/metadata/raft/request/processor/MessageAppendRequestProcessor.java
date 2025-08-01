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
import request.partition.MessageAppendRequest;
import request.partition.MessageAppendResponse;
import request.ErrorCode;

public class MessageAppendRequestProcessor implements RpcProcessor<MessageAppendRequest> {

  private PartitionManager partitionManager;
  public MessageAppendRequestProcessor() {
  }

  @Override
  public void handleRequest(RpcContext rpcCtx, MessageAppendRequest request) {

    PartitionRaftServer partitionRaftServer = partitionManager.getPartitionRaftServer(request.getGroupId());
    if (!partitionRaftServer.getNode().isLeader()) {
      MessageAppendResponse resp = new MessageAppendResponse();
      resp.setSuccess(false);
      resp.setErrorCode(ErrorCode.NOT_LEADER.getCode());
      resp.setErrorMsg(ErrorCode.NOT_LEADER.getDefaultMessage());
      rpcCtx.sendResponse(resp);
      return;
    }
    handleMessageAppendRequest(rpcCtx, (MessageAppendRequest) request, partitionRaftServer);
  }

  private void handleMessageAppendRequest(RpcContext rpcCtx, MessageAppendRequest request, PartitionRaftServer partitionRaftServer) {

    // Create a closure to handle the response after operation
    PartitionClosure done = new PartitionClosure(partitionRaftServer, request, status -> {
      MessageAppendResponse response = new MessageAppendResponse();
      if (status.isOk()) {
        response.setSuccess(true);
        response.setErrorCode(ErrorCode.SUCCESS.getCode());
      } else {
        response.setSuccess(false);
        response.setErrorCode(ErrorCode.INTERNAL_ERROR.getCode());
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
      MessageAppendResponse response = new MessageAppendResponse();
      response.setSuccess(false);
      response.setErrorCode(ErrorCode.INTERNAL_ERROR.getCode());
      response.setErrorMsg("Serialization failed: " + e.getMessage());
      rpcCtx.sendResponse(response);
    }
  }

  @Override
  public String interest() {
    return MessageAppendRequest.class.getName();
  }

  @Override
  public Executor executor() {
    return null; // Use default executor
  }

  public void setPartitionManager(PartitionManager partitionManager) {
    this.partitionManager = partitionManager;
  }
}
