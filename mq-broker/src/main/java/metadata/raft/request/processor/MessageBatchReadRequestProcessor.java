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

public class MessageBatchReadRequestProcessor implements RpcProcessor<MessageBatchReadRequest> {

  private PartitionManager partitionManager;
  public MessageBatchReadRequestProcessor() {
  }

  @Override
  public void handleRequest(RpcContext rpcCtx, MessageBatchReadRequest request) {
    PartitionRaftServer partitionRaftServer = partitionManager.getPartitionRaftServer(request.getGroupId());
    if (!partitionRaftServer.getNode().isLeader()) {
      // throw error
      rpcCtx.sendResponse("Not leader");
    }
    handleMessageBatchReadRequest(rpcCtx, (MessageBatchReadRequest) request, partitionRaftServer);
  }

  private void handleMessageBatchReadRequest(RpcContext rpcCtx, MessageBatchReadRequest request, PartitionRaftServer partitionRaftServer) {
    // Reads can be handled directly
    // only if leader, otherwise redirect to leader
    MessageBatchReadResponse response = partitionRaftServer.getStateMachine().handleBatchRead(request);
    rpcCtx.sendResponse(response);
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
