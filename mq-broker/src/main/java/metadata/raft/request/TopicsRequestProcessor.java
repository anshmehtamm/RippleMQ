package metadata.raft.request;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import metadata.raft.TopicsRaftServer;

/**
 * TopicsRequestProcessor handles incoming Raft requests,
 * distinguishing between read and write operations.
 */
public class TopicsRequestProcessor implements RpcProcessor<TopicsRequest> {

  private final TopicsRaftServer topicsRaftServer;

  /**
   * Constructor that accepts a TopicsRaftServer instance.
   *
   * @param topicsRaftServer The Raft server managing topics
   */
  public TopicsRequestProcessor(TopicsRaftServer topicsRaftServer) {
    this.topicsRaftServer = topicsRaftServer;
  }

  @Override
  public void handleRequest(RpcContext rpcCtx, TopicsRequest request) {
    if (!request.isWrite()) {
      // Handle read operation directly without Raft
      rpcCtx.sendResponse(topicsRaftServer.getStateMachine().getTopics());
    } else {
      // Handle write operations with Raft
      // Create a closure to handle the response after operation
      TopicsClosure done = new TopicsClosure(topicsRaftServer, request, new Closure() {
        @Override
        public void run(com.alipay.sofa.jraft.Status status) {
          if (status.isOk()) {
            rpcCtx.sendResponse("Success");
          } else {
            rpcCtx.sendResponse("Error: " + status.getErrorMsg());
          }
        }
      });

      try {
        // Serialize the request
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(request);
        oos.flush();
        byte[] data = baos.toByteArray();

        // Apply the request to the Raft node
        topicsRaftServer.getNode().apply(new Task(ByteBuffer.wrap(data), done));
      } catch (IOException e) {
        e.printStackTrace();
        rpcCtx.sendResponse("Serialization failed: " + e.getMessage());
      }
    }
  }

  @Override
  public String interest() {
    return TopicsRequest.class.getName();
  }

  @Override
  public Executor executor() {
    return null; // Use default executor
  }
}
