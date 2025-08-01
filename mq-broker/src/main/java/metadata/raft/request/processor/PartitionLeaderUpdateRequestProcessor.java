package metadata.raft.request.processor;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

import metadata.PartitionManager;
import metadata.raft.PartitionRaftServer;
import metadata.raft.TopicsRaftServer;
import request.partition.PartitionLeaderUpdateRequest;
import request.partition.PartitionLeaderUpdateResponse;
import request.ErrorCode;

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
      PartitionLeaderUpdateResponse resp = new PartitionLeaderUpdateResponse();
      resp.setSuccess(false);
      resp.setErrorCode(ErrorCode.NOT_LEADER.getCode());
      resp.setErrorMsg(ErrorCode.NOT_LEADER.getDefaultMessage());
      rpcCtx.sendResponse(resp);
      return;
    }
    handlePartitionLeaderUpdateRequest(rpcCtx, request);
  }

  private synchronized void handlePartitionLeaderUpdateRequest(RpcContext rpcCtx,
                                                  PartitionLeaderUpdateRequest request) {
    boolean success = partitionManager.handlePartitionLeaderChange(request.getGroupId(), request.getLeaderAddress(),
            true);
    PartitionLeaderUpdateResponse resp = new PartitionLeaderUpdateResponse();
    if (!success) {
      resp.setSuccess(false);
      resp.setErrorCode(ErrorCode.INTERNAL_ERROR.getCode());
      resp.setErrorMsg("Failed to update leader");
    } else {
      resp.setSuccess(true);
      resp.setErrorCode(ErrorCode.SUCCESS.getCode());
    }
    rpcCtx.sendResponse(resp);
  }

  @Override
  public String interest() {
    return PartitionLeaderUpdateRequest.class.getName();
  }

  public void setPartitionManager(PartitionManager partitionManager) {
    this.partitionManager = partitionManager;
  }
}
