package metadata.raft.request;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

import metadata.raft.PartitionRaftServer;
import request.partition.PartitionRequest;

/**
 * PartitionClosure handles the callback after a Raft write operation is completed.
 */
public class PartitionClosure implements Closure {

  private final PartitionRaftServer partitionRaftServer;
  private final PartitionRequest request;
  private final Closure done;

  /**
   * Constructor for PartitionClosure.
   *
   * @param partitionRaftServer The Raft server managing the partition
   * @param request             The original PartitionRequest
   * @param done                The Closure to notify upon completion
   */
  public PartitionClosure(PartitionRaftServer partitionRaftServer, PartitionRequest request, Closure done) {
    this.partitionRaftServer = partitionRaftServer;
    this.request = request;
    this.done = done;
  }

  @Override
  public void run(Status status) {
    if (done != null) {
      done.run(status);
    }
  }

  // Getters (optional)
}
