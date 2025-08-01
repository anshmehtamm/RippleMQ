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
import request.partition.MessageAppendRequest;
import request.partition.MessageAppendResponse;
import request.partition.MessageBatchReadRequest;
import request.partition.MessageBatchReadResponse;
import request.ErrorCode;

public class MessageBatchReadRequestProcessor implements RpcProcessor<MessageBatchReadRequest> {

  private PartitionManager partitionManager;
  public MessageBatchReadRequestProcessor() {
  }

  @Override
  public void handleRequest(RpcContext rpcCtx, MessageBatchReadRequest request) {
    PartitionRaftServer partitionRaftServer = partitionManager.getPartitionRaftServer(request.getGroupId());
    if (!partitionRaftServer.getNode().isLeader()) {
      MessageBatchReadResponse resp = new MessageBatchReadResponse();
      resp.setSuccess(false);
      resp.setErrorCode(ErrorCode.NOT_LEADER.getCode());
      resp.setErrorMsg(ErrorCode.NOT_LEADER.getDefaultMessage());
      rpcCtx.sendResponse(resp);
      return;
    }
    handleMessageBatchReadRequest(rpcCtx, (MessageBatchReadRequest) request, partitionRaftServer);
  }

  private void handleMessageBatchReadRequest(RpcContext rpcCtx, MessageBatchReadRequest request, PartitionRaftServer partitionRaftServer) {
    // Reads can be handled directly
    // only if leader, otherwise redirect to leader
    MessageBatchReadResponse response = partitionRaftServer.getStateMachine().handleBatchRead(request);
    response.setSuccess(true);
    response.setErrorCode(ErrorCode.SUCCESS.getCode());
    rpcCtx.sendResponse(response);
    System.err.println("Sent "+ response.getMessages().size() +" messages from "+request.getGroupId());
  }

  @Override
  public String interest() {
    return MessageBatchReadRequest.class.getName();
  }

  @Override
  public Executor executor() {
    return null; // Use default executor
  }

  public void setPartitionManager(PartitionManager partitionManager) {
    this.partitionManager = partitionManager;
  }
}
