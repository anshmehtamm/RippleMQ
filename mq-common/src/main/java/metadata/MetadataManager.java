package metadata;

import com.alipay.sofa.jraft.rpc.RpcClient;

import java.util.ArrayList;
import java.util.List;

import metadata.model.PartitionAssignment;
import metadata.model.Topic;

public class MetadataManager {

  private final List<Topic> topics;
  private final MetadataClient metadataClient;

  public MetadataManager(List<String> brokerAddresses) {
    this.topics = new ArrayList<>();
    this.metadataClient = new MetadataClient(brokerAddresses);
  }


  public synchronized void refreshMetadata(){
    List<Topic> topics = metadataClient.fetchMetadata();
    this.topics.clear();
    this.topics.addAll(topics);
    System.out.println("Metadata refreshed");
    for (Topic topic : topics) {
      System.out.println("Topic: " + topic.toString());
    }
  }

  public List<PartitionAssignment> getPartitionsForTopic(String topicName){
    if (topics.isEmpty()) {
      metadataClient.fetchMetadata();
    }
    for(Topic topic: topics){
      if(topic.getName().equals(topicName)){
        return topic.getPartitionAssignments();
      }
    }
    return null;
  }


  public RpcClient getRpcClient() {
    return metadataClient.getRpcClient();
  }
}
