# syntax=docker/dockerfile:1

# ------------------------------------------------------------------------
# Build stage: compile + assemble fat jar with sbt
# ------------------------------------------------------------------------
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.10_1.9.8_2.12.18 AS builder
WORKDIR /app

COPY project project
COPY build.sbt .
RUN sbt -Dsbt.ci=true update

COPY src src
RUN sbt -Dsbt.ci=true clean assembly && \
    cp /app/target/scala-2.12/spark-streaming-alerts-assembly.jar /tmp/app.jar

# ------------------------------------------------------------------------
# Runtime stage: Apache Spark 3.5.1 + our jar
# ------------------------------------------------------------------------
FROM apache/spark:3.5.1

USER root
RUN mkdir -p /opt/app /opt/checkpoints && \
    chown -R spark:spark /opt/app /opt/checkpoints

COPY --from=builder /tmp/app.jar /opt/app/app.jar
RUN chown spark:spark /opt/app/app.jar

USER spark
WORKDIR /opt/app

ENV SPARK_MASTER=local[*] \
    KAFKA_BROKERS=kafka:9092 \
    TOPIC_GEOLOCATION=topic.geolocation \
    TOPIC_ALERT=topic.alert \
    CHECKPOINT_DIR=/opt/checkpoints/alerts \
    RISK_THRESHOLD=0.10 \
    STARTING_OFFSETS=latest \
    TRIGGER_INTERVAL=10\ seconds

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--master", "local[*]", \
  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
  "--conf", "spark.driver.memory=1g", \
  "--conf", "spark.executor.memory=1g", \
  "--class", "com.ridesim.streaming.Main", \
  "/opt/app/app.jar"]
