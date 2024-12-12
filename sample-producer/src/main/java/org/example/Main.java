package org.example;

import java.util.Arrays;

import client.ProducerClient;
import client.ProducerClientImpl;

public class Main {

    public static void main(String[] args) {

        // Initialize the producer client with a list of broker addresses
        ProducerClient producerClient = new ProducerClientImpl(
                "client1",
                Arrays.asList("localhost:9092", "localhost:9093", "localhost:9094",
                        "localhost:9095", "localhost:9096"));

        // Produce messages in a separate thread
        new Thread(() -> {
            try {
                produceMessages(producerClient);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        // Keep the application alive
        keepAlive();
    }

    private static void produceMessages(ProducerClient producerClient) throws InterruptedException {
        int totalMessages = 2;
        while (totalMessages-- > 0) {
            producerClient.produce("topic1", "test-message");
            System.out.println("Message sent to topic1: test-message");
            Thread.sleep(1000);
        }
    }

    private static void keepAlive() {
        // Keep alive
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}