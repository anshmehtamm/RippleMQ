package topics.raft.topics.request;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

import topics.raft.topics.TopicsRaftServer;

/**
 * TopicsClosure handles the callback after a Raft write operation is completed.
 */
public class TopicsClosure implements Closure {

  private final TopicsRaftServer topicsRaftServer;
  private final TopicsRequest request;
  private final TopicsResponse response;
  private final Closure done;

  /**
   * Constructor for TopicsClosure.
   *
   * @param topicsRaftServer The Raft server managing topics
   * @param request          The original TopicsRequest
   * @param response         The TopicsResponse to be populated
   * @param done             The original Closure to notify upon completion
   */
  public TopicsClosure(TopicsRaftServer topicsRaftServer, TopicsRequest request, TopicsResponse response, Closure done) {
    this.topicsRaftServer = topicsRaftServer;
    this.request = request;
    this.response = response;
    this.done = done;
  }

  @Override
  public void run(Status status) {
    if (status.isOk()) {
      response.setSuccess(true);
    } else {
      response.setSuccess(false);
      response.setErrorMsg(status.getErrorMsg());
    }
    if (done != null) {
      done.run(status);
    }
  }

  // Getters (optional)
  public TopicsRaftServer getTopicsRaftServer() {
    return topicsRaftServer;
  }

  public TopicsRequest getRequest() {
    return request;
  }

  public TopicsResponse getResponse() {
    return response;
  }

  public Closure getDone() {
    return done;
  }
}
