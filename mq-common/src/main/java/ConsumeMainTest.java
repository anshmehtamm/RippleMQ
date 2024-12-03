import java.util.Arrays;
import java.util.List;

import client.ConsumerClient;
import client.ConsumerClientImpl;

public class ConsumeMainTest {

  public static void main(String[] args) {
     ConsumerClient consumerClient = new ConsumerClientImpl("client2",
             Arrays.asList("localhost:9092", "localhost:9093", "localhost:9094",
                     "localhost:9095", "localhost:9096"));


      // consume every second 10 message from topic1 / topic2
      new Thread(() -> {

        while (true) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          // random 0 or 1
          int topic = (int) (Math.random() * 2);
          List<String> messages = null;
          if (topic == 0) {
            messages = consumerClient.consume("topic1");
          } else {
            messages = consumerClient.consume("topic2");
          }

          if (messages != null) {
            messages.forEach(System.out::println);
          }
        }
      }).start();

  }


}
