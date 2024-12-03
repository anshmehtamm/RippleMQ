import java.util.Arrays;

import client.ProducerClient;
import client.ProducerClientImpl;

public class ProducerMainTest {

  public static void main(String[] args) {

    ProducerClient producerClient = new ProducerClientImpl(
            "client1",
            Arrays.asList("localhost:9092", "localhost:9093", "localhost:9094",
                    "localhost:9095", "localhost:9096"));

    // produce every second 10 message to topic1 / topic2

    new Thread(() -> {
      int i = 0;
      while (i<10) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        // random 0 or 1
        int topic = (int) (Math.random() * 2);
        if (topic == 0) {
          producerClient.produce("topic1", "test-message");
        } else {
          producerClient.produce("topic2", "test-message");
        }
        i++;
      }

    }).start();


    keepAlive();
  }

  private static void keepAlive() {
    // keep alive
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}

