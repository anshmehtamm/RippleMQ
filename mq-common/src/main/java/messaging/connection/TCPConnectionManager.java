package messaging.connection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import messaging.Message;
import messaging.MessageFactory;
import messaging.MessageSender;

public class TCPConnectionManager {

  private final int peerId;
  public static final int BASE_PORT = 9000;
  private Map<Integer, Connection> connections;
  private Map<Integer, Queue<Message>> unsentMessageQueue;
  private Map<Integer, String> peers;

  public ExecutorService executorService = Executors.newCachedThreadPool();
  private MessageSender messageDispatcher;

  public TCPConnectionManager(int peerId, Map<Integer, String> peers, MessageSender messageDispatcher) {
    this.peerId = peerId;
    this.peers = peers;
    this.connections = new ConcurrentHashMap<>();
    this.unsentMessageQueue = new ConcurrentHashMap<>();
    this.messageDispatcher = messageDispatcher;

    connectToPeers();
    startListening();
  }

  public void makeNewConnection(int peerId, String hostname) {
    peers.put(peerId, hostname);
    attemptConnectToPeer(peerId);
  }

  private void startListening() {
    executorService.submit(() -> {
      try (ServerSocket serverSocket = new ServerSocket(BASE_PORT)) {
        while (true) {
          Socket clientSocket = serverSocket.accept();
          formConnectionToAcceptedPeer(clientSocket);
        }
      } catch (IOException e) {
        // ignore, keep listening
      }
    });
  }

  private void formConnectionToAcceptedPeer(Socket socket) {
    executorService.submit(() -> {
      try {
        DataInputStream dis = new DataInputStream(socket.getInputStream());
        int fromId = dis.readInt();

        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(this.peerId);
        dos.flush();

        handleConnection(socket, fromId, dis, dos);


      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void handleConnection(Socket socket, int fromId, DataInputStream dis, DataOutputStream dos) {
    Connection peerConnection = new Connection(fromId, socket, dis, dos);
    connections.put(fromId, peerConnection);

    // thread to receive messages
    executorService.submit(() -> receiveMessages(peerConnection));

    sendUnsentMessages(fromId, peerConnection);
  }

  private void receiveMessages(Connection con){
    try {
      while (true) {
        int messageLength = con.in.readInt();
        byte[] buffer = new byte[messageLength];
        con.in.readFully(buffer);
        ByteArrayInputStream bais = new ByteArrayInputStream(buffer);
        DataInputStream messageDis = new DataInputStream(bais);

        // Deserialize the message
        Message message = MessageFactory.deserializeMessage(messageDis);
        messageDispatcher.dispatchMessage(message, con.peerId);
      }
    } catch (IOException e) {
      System.err.println("Connection to peer: " + con.peerId + " lost");
      connections.remove(con.peerId);
      // retry connecting to peer
      attemptConnectToPeer(con.peerId);
    }
  }

  public void sendMessage(Message message, int toId) {
    Connection peerConnection = connections.get(toId);
    if (peerConnection != null) {
      sendMessageToPeer(message, peerConnection);
    } else {
      unsentMessageQueue.computeIfAbsent(toId, k -> new LinkedBlockingQueue<>()).offer(message);
    }
  }

  private void sendMessageToPeer(Message message, Connection peerConnection) {
    executorService.submit(() -> {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream messageDos = new DataOutputStream(baos);

        // Serialize the message
        MessageFactory.serializeMessage(message, messageDos);
        messageDos.flush();
        byte[] serializedMessage = baos.toByteArray();
        peerConnection.out.writeInt(serializedMessage.length);
        peerConnection.out.write(serializedMessage);
        peerConnection.out.flush();
      } catch (IOException e) {
        System.err.println("Connection to peer: " + peerConnection.peerId + " lost");
        // add message to unsent queue
        connections.remove(peerConnection.peerId);
        unsentMessageQueue.computeIfAbsent(peerConnection.peerId, k -> new LinkedBlockingQueue<>()).offer(message);
        // retry connecting to peer
        attemptConnectToPeer(peerConnection.peerId);
      }
    });
  }


  private void connectToPeers() {
    for (Integer toConnectPeerId : peers.keySet()) {
      if (toConnectPeerId > this.peerId) {
        attemptConnectToPeer(toConnectPeerId);
      }
    }
  }

  private void attemptConnectToPeer(int toConnectPeerId) {
    executorService.submit(() -> {
      String hostname = peers.get(toConnectPeerId);
      while (true) {
        try {
          Socket socket = new Socket(hostname, BASE_PORT);

          DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
          dos.writeInt(this.peerId);
          dos.flush();

          DataInputStream dis = new DataInputStream(socket.getInputStream());
          int fromId = dis.readInt();

          if (fromId != toConnectPeerId) {
            throw new IOException("Unexpected peer id: " + fromId);
          }

          handleConnection(socket, fromId, dis, dos);
          break;

        } catch (IOException e) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    });
  }

  private void sendUnsentMessages(int toConnectPeerId, Connection peerConnection) {
    Queue<Message> unsentMessages = unsentMessageQueue.remove(toConnectPeerId);
    if (unsentMessages != null) {
      for (Message message : unsentMessages) {
        sendMessageToPeer(message, peerConnection);
      }
    }
  }


  private static class Connection {
    int peerId;
    Socket socket;
    DataInputStream in;
    DataOutputStream out;

    Connection(int peerId, Socket socket, DataInputStream in, DataOutputStream out) {
      this.peerId = peerId;
      this.socket = socket;
      this.in = in;
      this.out = out;
    }
  }
}
