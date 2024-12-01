package partition.raft.request;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import partition.raft.PartitionRaftServer;

/**
 * PartitionRequestProcessor handles incoming requests,
 * including message append, batch read, and consumer offset updates.
 */
public class PartitionRequestProcessor implements RpcProcessor<PartitionRequest> {

  private final PartitionRaftServer partitionRaftServer;
  private final String groupId;

  /**
   * Constructor that accepts a PartitionRaftServer instance.
   *
   * @param partitionRaftServer The Raft server managing the partition
   */
  public PartitionRequestProcessor(PartitionRaftServer partitionRaftServer, String groupId) {
    this.partitionRaftServer = partitionRaftServer;
    this.groupId = groupId;
  }

  @Override
  public void handleRequest(RpcContext rpcCtx, PartitionRequest request) {
    if (!partitionRaftServer.getNode().isLeader()) {
      // throw error
      rpcCtx.sendResponse("Not leader");
    }
    if (request instanceof MessageAppendRequest) {
      handleMessageAppendRequest(rpcCtx, (MessageAppendRequest) request);
    } else if (request instanceof MessageBatchReadRequest) {
      handleMessageBatchReadRequest(rpcCtx, (MessageBatchReadRequest) request);
    } else if (request instanceof ConsumerOffsetUpdateRequest) {
      handleConsumerOffsetUpdateRequest(rpcCtx, (ConsumerOffsetUpdateRequest) request);
    } else {
      // Unknown request type
      rpcCtx.sendResponse("Unknown request type");
    }
  }

  private void handleMessageAppendRequest(RpcContext rpcCtx, MessageAppendRequest request) {

    // Create a closure to handle the response after operation
    PartitionClosure done = new PartitionClosure(partitionRaftServer, request, status -> {
      MessageAppendResponse response = new MessageAppendResponse();
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
      MessageAppendResponse response = new MessageAppendResponse();
      response.setSuccess(false);
      response.setErrorMsg("Serialization failed: " + e.getMessage());
      rpcCtx.sendResponse(response);
    }
  }

  private void handleMessageBatchReadRequest(RpcContext rpcCtx, MessageBatchReadRequest request) {
    // Reads can be handled directly
    // only if leader, otherwise redirect to leader
    MessageBatchReadResponse response = partitionRaftServer.getStateMachine().handleBatchRead(request);
    rpcCtx.sendResponse(response);
  }

  private void handleConsumerOffsetUpdateRequest(RpcContext rpcCtx, ConsumerOffsetUpdateRequest request) {
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
    return groupId;
  }

  @Override
  public Executor executor() {
    return null; // Use default executor
  }
}
