# Broker Service

## Components

1. Broker Cluste Manager
3. Topics State Machine
4. Partition Manager
5. Partition Assigner


## Flow



## Configuration

Use ```cluster_config.yaml``` for specifying

1. List of brokers in the cluster
2. List of Topic and for each topic the number of partitions and replication_factor.

```yaml
brokers:
  - id: 1
    hostname: 'broker1'
    port: 9092
  - id: 2
    hostname: 'broker2'
    port: 9092
  - id: 3
    hostname: 'broker3'
    port: 9092
  - id: 4
    hostname: 'broker4'
    port: 9092
  - id: 5
    hostname: 'broker5'
    port: 9092
topics:
  - name: 'topic1'
    partitions: 3
    replicationFactor: 3
  - name: 'topic2'
    partitions: 3
    replicationFactor: 3
```

## Setup

1. Build docker image

```   docker build -t mq-broker -f mq-broker/Dockerfile . ```
   
2. Docker compose for multiple brokers

 ``` docker compose -f mq-broker/docker-compose.yml up ```
