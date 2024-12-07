# RippleMQ

## Notes

In development by [Shail Shah
](https://github.com/shailshah76) and [Ansh Mehta](https://anshmehtamm.github.io/personal-website/)

Follow project status at https://github.com/users/anshmehtamm/projects/1

## About
RippleMQ is a simplified version of a Distributed Messaging Queue inspired by Kafka, focusing on implementing its core functionalities. 
This project is developed as part of CS 7610 – Foundations of Distributed Systems @ Northeastern

## Architecture

<img width="480" alt="image" src="https://github.com/user-attachments/assets/a1a9a567-a389-4088-86fd-858316857716">


## Modules

1. **mq-broker**: Module for broker service. Deploy multiple broker services to form a cluster of RippleMQ. 
2. **mq-common**: Module for implementing producers and consumers, provides API for them.

### How to implement producer and consumer

Add below dependency in your **_POM.xml_**
```xml
<dependency>
  <groupId>org.example</groupId>
   <artifactId>mq-common</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```
Use below to implement producer
```java
ProducerClient producerClient = new ProducerClientImpl(
        "client1",
        Arrays.asList("localhost:9092", "localhost:9093", "localhost:9094",
                "localhost:9095", "localhost:9096"));

producerClient.produce("topic1", "test-message");
```

Use below to implement consumer
```java
 ConsumerClient consumerClient = new ConsumerClientImpl("client3",
         Arrays.asList("localhost:9092", "localhost:9093", "localhost:9094",
                 "localhost:9095", "localhost:9096"));

List<String> messages = consumerClient.consume("topic1");
```
## Current Functionalities

1. Cluster RAFT Service
   - Metadata Management
   - Partition Assignment
2. Partition RAFT Service
   - Partition Replication
   - Offset Management
  

## Setup

To-be-added

## Contributing

        
