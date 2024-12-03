package client;

import java.util.List;

public interface ConsumerClient {

  public List<String> consume(String topic);
}
