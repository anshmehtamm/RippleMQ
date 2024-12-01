package metadata.raft;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.PeerId;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import partition.PartitionManager;
import partition.Topic;
import metadata.raft.request.TopicsRequest;


/**
 * TopicsStateMachine manages the state of topics in the Raft cluster.
 */
public class TopicsStateMachine extends StateMachineAdapter {

  private List<Topic> topics = new ArrayList<>();
  private PartitionManager partitionManager;
  private PeerId selfId;

  public TopicsStateMachine(PartitionManager partitionManager, PeerId selfId) {
    this.partitionManager = partitionManager;
    this.selfId = selfId;
  }

  @Override
  public void onLeaderStart(final long term) {
    super.onLeaderStart(term);
    partitionManager.handleLeaderChange(selfId);
  }

  @Override
  public void onLeaderStop(final Status status) {
    super.onLeaderStop(status);
    partitionManager.handleLeaderChange(null);
  }

  /**
   * Sets the list of topics and notifies the PartitionManager.
   *
   * @param newTopics The new list of topics
   */
  public synchronized void setTopics(List<Topic> newTopics) {
    this.topics = newTopics;
    // Notify PartitionManager about topic list change
    partitionManager.handleTopicListChange(newTopics);
  }

  // Synchronized to handle concurrent access
  public synchronized List<Topic> getTopics() {
    return new ArrayList<>(topics);
  }

  @Override
  public void onApply(Iterator iterator) {
    while (iterator.hasNext()) {
      final ByteBuffer data = iterator.getData();
      try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data.array()))) {
        TopicsRequest request = (TopicsRequest) ois.readObject();
        if (request.isWrite() && request.getTopics() != null) {
          setTopics(request.getTopics());
        }
      } catch (IOException | ClassNotFoundException e) {
        e.printStackTrace();
        // Log the error; in a production system, consider more robust error handling
      }
      iterator.next();
    }
  }

  public void setPartitionManager(PartitionManager partitionManager) {
    this.partitionManager = partitionManager;
  }
}
