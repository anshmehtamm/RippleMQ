package request.partition;

import java.io.Serializable;

public class PartitionLeaderUpdateRequest implements Serializable {



  private static final long serialVersionUID = 1L;

  private String groupId;
  private String leaderAddress;


  public PartitionLeaderUpdateRequest(String groupId, String leaderAddress) {
    this.groupId = groupId;
    this.leaderAddress = leaderAddress;
  }

  public String getGroupId() {
    return groupId;
  }

  public String getLeaderAddress() {
    return leaderAddress;
  }

  @Override
  public String toString() {
    return "PartitionLeaderUpdateRequest{groupId='" + groupId + "', leaderAddress='" + leaderAddress + "'}";
  }




}
