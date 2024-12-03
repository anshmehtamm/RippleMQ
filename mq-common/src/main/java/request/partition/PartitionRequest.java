package request.partition;

import java.io.Serializable;

/**
 * PartitionRequest is a marker interface for all partition requests.
 */
public interface PartitionRequest extends Serializable {

  public String getGroupId();

}
