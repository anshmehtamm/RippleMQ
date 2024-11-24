package app;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.LifeCycle;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class MetadataStateMachine implements StateMachine {
  @Override
  public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage raftStorage) throws IOException {

  }

  @Override
  public LifeCycle.State getLifeCycleState() {
    return null;
  }

  @Override
  public void pause() {

  }

  @Override
  public void reinitialize() throws IOException {

  }

  @Override
  public long takeSnapshot() throws IOException {
    return 0;
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return null;
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    return null;
  }

  @Override
  public CompletableFuture<Message> query(Message message) {
    return null;
  }

  @Override
  public CompletableFuture<Message> queryStale(Message message, long l) {
    return null;
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest raftClientRequest) throws IOException {
    return null;
  }

  @Override
  public TransactionContext preAppendTransaction(TransactionContext transactionContext) throws IOException {
    return null;
  }

  @Override
  public TransactionContext cancelTransaction(TransactionContext transactionContext) throws IOException {
    return null;
  }

  @Override
  public TransactionContext applyTransactionSerial(TransactionContext transactionContext) throws InvalidProtocolBufferException {
    return null;
  }

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext transactionContext) {
    return null;
  }

  @Override
  public TermIndex getLastAppliedTermIndex() {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
