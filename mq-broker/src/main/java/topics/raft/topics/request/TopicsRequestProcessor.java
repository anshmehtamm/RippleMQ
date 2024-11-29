package topics.raft.topics.request;

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

import topics.raft.topics.TopicsRaftServer;

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
      TopicsResponse response = new TopicsResponse();
      response.setSuccess(true);
      response.setTopics(topicsRaftServer.getStateMachine().getTopics());
      rpcCtx.sendResponse(response);
    } else {
      // Handle write operations with Raft
      TopicsResponse response = new TopicsResponse();

      // Create a closure to handle the response after operation
      TopicsClosure done = new TopicsClosure(topicsRaftServer, request, response, new Closure() {
        @Override
        public void run(Status status) {
          if (status.isOk()) {
            response.setSuccess(true);
          } else {
            response.setSuccess(false);
            response.setErrorMsg(status.getErrorMsg());
          }
          rpcCtx.sendResponse(response);
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
        topicsRaftServer.getRaftGroupService().getRaftNode().apply(new Task(ByteBuffer.wrap(data), done));
      } catch (IOException e) {
        e.printStackTrace();
        response.setSuccess(false);
        response.setErrorMsg("Serialization failed: " + e.getMessage());
        rpcCtx.sendResponse(response);
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
