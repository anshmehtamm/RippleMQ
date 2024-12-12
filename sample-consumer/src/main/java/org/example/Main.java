package org.example;

import java.util.Arrays;
import java.util.List;

import client.ConsumerClient;
import client.ConsumerClientImpl;

public class Main {

    public static void main(String[] args) {
        // Initialize the consumer client with a list of broker addresses
        ConsumerClient consumerClient = new ConsumerClientImpl("client2",
                Arrays.asList("localhost:9092", "localhost:9093", "localhost:9094",
                        "localhost:9095", "localhost:9096"));

        // Start a thread to consume messages every second
        new Thread(() -> {
            while (true) {
                try {
                    // Wait for 1 second between message consumptions
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // Randomly select topic1 or topic2
                int topic = (int) (Math.random() * 2);
                List<String> messages = null;

                if (topic == 0) {
                    messages = consumerClient.consume("topic1");
                } else {
                    messages = consumerClient.consume("topic2");
                }

                // Print consumed messages
                if (messages != null) {
                    messages.forEach(System.out::println);
                }
            }
        }).start();

        // Keep the application alive
        keepAlive();
    }

    private static void keepAlive() {
        // Prevent the application from exiting
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
