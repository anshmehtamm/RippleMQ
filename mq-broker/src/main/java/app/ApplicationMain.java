package app;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class ApplicationMain {
  public static void main(String[] args) throws IOException {

    int id = Integer.parseInt(args[1]);

    ClusterConfig config = ClusterConfig.loadConfig("mq-broker/config/cluster_config.yaml");

    RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(new File("mq-broker/data")));
    System.out.println("Starting broker with id " + id);
    Broker self = config.getSelf(id);
    if (self == null) {
      throw new IllegalArgumentException("Broker with id " + id + " not found in the configuration");
    }
    RaftPeer peer = RaftPeer.newBuilder()
      .setId(""+self.getId())
      .setAddress(new InetSocketAddress(self.getHost(), config.getPort()))
      .build();

    RaftPeer[] others = config.getBrokers().stream()
      .map(broker -> RaftPeer.newBuilder()
        .setId(""+broker.getId())
        .setAddress(new InetSocketAddress(broker.getHost(), config.getPort()))
        .build())
      .toArray(RaftPeer[]::new);

    List<RaftPeer> peers = new ArrayList<>();
    peers.addAll(Arrays.asList(others));

    RaftGroup group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.fromString("02511d47-d67c-49a3-9011-abb3109a44c1")), peers);
    final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    RaftServer server = RaftServer.newBuilder().setServerId(peer.getId())
      .setStateMachine(new MetadataStateMachine())
      .setGroup(group)
      .setProperties(properties)
      .build();

    server.start();
  }
}
