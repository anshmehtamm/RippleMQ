package partition.raft;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import request.partition.ConsumerOffsetUpdateRequest;
import request.partition.MessageAppendRequest;
import request.partition.MessageBatchReadRequest;
import request.partition.MessageBatchReadResponse;

/**
 * PartitionStateMachine manages the state of the partition,
 * including messages and consumer offsets.
 */
public class PartitionStateMachine extends StateMachineAdapter {

  private final List<String> messages = new ArrayList<>();
  private final Map<String, Long> consumerOffsets = new HashMap<>();
  private final String groupId;
  public PartitionStateMachine(String groupId) {
    this.groupId = groupId;
  }

  @Override
  public void onApply(Iterator iterator) {
    while (iterator.hasNext()) {
      ByteBuffer data = iterator.getData();
      if (data != null) {
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
          Object obj = ois.readObject();
          if (obj instanceof MessageAppendRequest) {
            MessageAppendRequest request = (MessageAppendRequest) obj;
            // Append messages to the partition
            synchronized (messages) {
              messages.addAll(request.getMessages());
            }
          } else if (obj instanceof ConsumerOffsetUpdateRequest) {
            ConsumerOffsetUpdateRequest request = (ConsumerOffsetUpdateRequest) obj;
            // Update consumer offset
            synchronized (consumerOffsets) {
              consumerOffsets.put(request.getConsumerId(), request.getOffset());
            }
          } else {
            // Unknown request
            System.err.println("Unknown request type in onApply: " + obj.getClass());
          }
        } catch (IOException | ClassNotFoundException e) {
          e.printStackTrace();
        }
      }
      if (iterator.done() != null) {
        iterator.done().run(Status.OK());
      }
      iterator.next();
    }
  }

  /**
   * Handles batch read request. Since reads are not modifying state, they can be handled directly.
   *
   * @param request The batch read request
   * @return The batch read response containing messages and offset
   */
  public MessageBatchReadResponse handleBatchRead(MessageBatchReadRequest request) {
    String consumerId = request.getConsumerId();
    int maxMessages = request.getMaxMessages();
    long offset;
    synchronized (consumerOffsets) {
      offset = consumerOffsets.getOrDefault(consumerId, 0L);
    }

    List<String> messagesToReturn = new ArrayList<>();
    synchronized (messages) {
      int endIndex = (int) Math.min(offset + maxMessages, messages.size());
      for (int i = (int) offset; i < endIndex; i++) {
        messagesToReturn.add(messages.get(i));
      }
    }
    // Return the messages and the current offset
    MessageBatchReadResponse response = new MessageBatchReadResponse();
    response.setMessages(messagesToReturn);
    response.setOffset(offset);
    return response;
  }

  public long getConsumerOffset(String consumerId) {
    synchronized (consumerOffsets) {
      return consumerOffsets.getOrDefault(consumerId, 0L);
    }
  }

  @Override
  public void onLeaderStart(long term) {
    super.onLeaderStart(term);
    System.out.println("PartitionStateMachine: Leader started for partition " + groupId);
  }
}
