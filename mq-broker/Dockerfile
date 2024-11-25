# Use a base image with Java 11 runtime
FROM openjdk:11-jre-slim

# Set the working directory in the container
WORKDIR /app

COPY config/cluster_config.yaml /app/mq-broker/config/cluster_config.yaml

RUN mkdir /app/logs

# Copy the jar file into the container
COPY target/mq-broker-1.0-SNAPSHOT.jar /app/mq-broker.jar

# Copy configuration files (if needed)
COPY config/ /app/config/

# Set the default command to run the application
ENTRYPOINT ["java", "-jar", "/app/mq-broker.jar"]