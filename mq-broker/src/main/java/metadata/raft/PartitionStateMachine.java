package metadata.raft;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import metadata.PartitionManager;
import request.partition.ConsumerOffsetUpdateRequest;
import request.partition.MessageAppendRequest;
import request.partition.MessageBatchReadRequest;
import request.partition.MessageBatchReadResponse;

/**
 * PartitionStateMachine manages the state of the partition,
 * including messages and consumer offsets.
 */
public class PartitionStateMachine extends StateMachineAdapter {
  private static final Logger logger = LoggerFactory.getLogger(PartitionStateMachine.class);

  private final List<String> messages = new ArrayList<>();
  private final Map<String, Long> consumerOffsets = new HashMap<>();
  private final String groupId;
  private PartitionManager partitionManager;

  public PartitionStateMachine(String groupId, PartitionManager partitionManager) {
    this.groupId = groupId;
    this.partitionManager = partitionManager;
    logger.info("Initialized PartitionStateMachine for group: {}", groupId);
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
            handleMessageAppendRequest((MessageAppendRequest) obj);
          } else if (obj instanceof ConsumerOffsetUpdateRequest) {
            handleConsumerOffsetUpdateRequest((ConsumerOffsetUpdateRequest) obj);
          } else {
            logger.warn("Unknown request type in onApply: {}", obj.getClass());
          }
        } catch (IOException | ClassNotFoundException e) {
          logger.error("Error processing request in onApply", e);
        }
      }
      if (iterator.done() != null) {
        iterator.done().run(Status.OK());
      }
      iterator.next();
    }
  }

  private void handleMessageAppendRequest(MessageAppendRequest request) {
    synchronized (messages) {
      messages.addAll(request.getMessages());
      logger.debug("Message queue for group {} size is now: {}", groupId, messages.size());
    }
  }

  private void handleConsumerOffsetUpdateRequest(ConsumerOffsetUpdateRequest request) {
    synchronized (consumerOffsets) {
      consumerOffsets.put(request.getConsumerId(), request.getOffset());
      logger.debug("Updated offset for consumer {} to {} in group {}",
              request.getConsumerId(), request.getOffset(), groupId);
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
      logger.debug("Reading messages for consumer {} from offset {} in group {}",
              consumerId, offset, groupId);
    }

    List<String> messagesToReturn = new ArrayList<>();
    synchronized (messages) {
      int endIndex = (int) Math.min(offset + maxMessages, messages.size());
      for (int i = (int) offset; i < endIndex; i++) {
        messagesToReturn.add(messages.get(i));
      }
      logger.debug("Retrieved {} messages for consumer {} in group {}",
              messagesToReturn.size(), consumerId, groupId);
    }

    MessageBatchReadResponse response = new MessageBatchReadResponse();
    response.setMessages(messagesToReturn);
    response.setOffset(offset);
    return response;
  }

  public long getConsumerOffset(String consumerId) {
    synchronized (consumerOffsets) {
      long offset = consumerOffsets.getOrDefault(consumerId, 0L);
      logger.trace("Retrieved offset {} for consumer {} in group {}",
              offset, consumerId, groupId);
      return offset;
    }
  }

  @Override
  public void onLeaderStart(long term) {
    super.onLeaderStart(term);
    logger.info("Leader started for partition {} with term {}", groupId, term);
    partitionManager.handlePartitionLeaderChange(groupId);
  }
}