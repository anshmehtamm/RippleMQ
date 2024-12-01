# Stage 1: Build the application using Maven
FROM maven:3.8.6-openjdk-11 AS build

# Set the working directory inside the build container
WORKDIR /app

# Copy the pom.xml and download dependencies (this layer is cached if dependencies don't change)
COPY pom.xml .

# Download dependencies
RUN mvn dependency:go-offline

# Copy the source code
COPY src ./src
COPY config ./config

# Package the application
RUN mvn --batch-mode -T 1C package -DskipTests

# Stage 2: Create the runtime image
FROM openjdk:11-jre-slim

# Set the working directory in the container
WORKDIR /app

# Create necessary directories
RUN mkdir -p /app/mq-broker/config /app/logs

# Copy the JAR file from the build stage
COPY --from=build /app/target/mq-broker-1.0-SNAPSHOT.jar /app/mq-broker.jar

# Copy configuration files from the build stage (if they are needed at runtime)
COPY --from=build /app/config/ /app/config/
COPY config/cluster_config.yaml /app/mq-broker/config/cluster_config.yaml

# Set the default command to run the application
ENTRYPOINT ["java", "-jar", "/app/mq-broker.jar"]
