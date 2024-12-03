# Stage 1: Build mq-common using Maven
FROM maven:3.8.6-openjdk-11 AS mq-common-build

# Set the working directory for mq-common
WORKDIR /mq-common

# Copy mq-common's pom.xml and source code
COPY mq-common/pom.xml .
RUN mvn dependency:go-offline
COPY mq-common/src ./src

# Build and install mq-common JAR into the local Maven repository
RUN mvn clean install -DskipTests

# Stage 2: Build mq-broker using Maven
FROM maven:3.8.6-openjdk-11 AS mq-broker-build

# Set the working directory for mq-broker
WORKDIR /mq-broker


# Copy the local Maven repository from mq-common-build stage
COPY --from=mq-common-build /root/.m2 /root/.m2

# Copy mq-broker's pom.xml and source code
COPY mq-broker/pom.xml .
RUN mvn dependency:go-offline
COPY mq-broker/src ./src
COPY mq-broker/config ./config


# Build mq-broker
RUN mvn clean package -DskipTests

# Stage 3: Create the runtime image
FROM openjdk:11-jre-slim

# Set the working directory in the runtime container
WORKDIR /app

# Create necessary directories
RUN mkdir -p /app/mq-broker/config /app/logs

# Copy the JAR file from the mq-broker build stage
COPY --from=mq-broker-build /mq-broker/target/mq-broker-1.0-SNAPSHOT.jar /app/mq-broker.jar

# Copy runtime configuration files from mq-broker build stage
COPY --from=mq-broker-build /mq-broker/config/ /app/config/
COPY mq-broker/config/cluster_config.yaml /app/mq-broker/config/cluster_config.yaml

# Set the default command to run the application
ENTRYPOINT ["java", "-jar", "/app/mq-broker.jar"]
