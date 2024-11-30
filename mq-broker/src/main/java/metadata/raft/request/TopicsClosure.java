package metadata.raft.request;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

import metadata.raft.TopicsRaftServer;

/**
 * TopicsClosure handles the callback after a Raft write operation is completed.
 */
public class TopicsClosure implements Closure {

  private final TopicsRaftServer topicsRaftServer;
  private final TopicsRequest request;
  private final Closure done;

  /**
   * Constructor for TopicsClosure.
   *
   * @param topicsRaftServer The Raft server managing topics
   * @param request          The original TopicsRequest
   * @param done             The original Closure to notify upon completion
   */
  public TopicsClosure(TopicsRaftServer topicsRaftServer, TopicsRequest request, Closure done) {
    this.topicsRaftServer = topicsRaftServer;
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
  public TopicsRaftServer getTopicsRaftServer() {
    return topicsRaftServer;
  }

  public TopicsRequest getRequest() {
    return request;
  }

  public Closure getDone() {
    return done;
  }
}
