package metadata.raft.request.processor;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

import metadata.PartitionManager;
import metadata.raft.PartitionRaftServer;
import metadata.raft.TopicsRaftServer;
import request.partition.PartitionLeaderUpdateRequest;

public class PartitionLeaderUpdateRequestProcessor implements
        RpcProcessor<PartitionLeaderUpdateRequest> {

  private PartitionManager partitionManager;
  private TopicsRaftServer topicsRaftServer;

  public PartitionLeaderUpdateRequestProcessor(TopicsRaftServer
                                               topicsRaftServer) {
    this.topicsRaftServer = topicsRaftServer;
  }

  @Override
  public void handleRequest(RpcContext rpcCtx, PartitionLeaderUpdateRequest request) {
    // Get the partition Raft server
    if (!topicsRaftServer.getNode().isLeader()){
      // throw error
      rpcCtx.sendResponse("Not leader");
    }
    handlePartitionLeaderUpdateRequest(rpcCtx, request);
  }

  private synchronized void handlePartitionLeaderUpdateRequest(RpcContext rpcCtx,
                                                  PartitionLeaderUpdateRequest request) {
    boolean success = partitionManager.handlePartitionLeaderChange(request.getGroupId(), request.getLeaderAddress(),
            true);
    if (!success) {
      rpcCtx.sendResponse("Error");
      return;
    }
    rpcCtx.sendResponse("Success");
  }

  @Override
  public String interest() {
    return PartitionLeaderUpdateRequest.class.getName();
  }

  public void setPartitionManager(PartitionManager partitionManager) {
    this.partitionManager = partitionManager;
  }
}
