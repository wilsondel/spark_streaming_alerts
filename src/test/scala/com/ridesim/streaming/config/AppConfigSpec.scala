package com.ridesim.streaming.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AppConfigSpec extends AnyFlatSpec with Matchers {

  "AppConfig.fromEnv" should "return all defaults when env is empty" in {
    val cfg = AppConfig.fromEnv(Map.empty)
    cfg.kafkaBrokers     shouldBe "localhost:9092"
    cfg.topicGeolocation shouldBe "topic.geolocation"
    cfg.topicLifecycle   shouldBe "topic.lifecycle"
    cfg.topicAlert       shouldBe "topic.alert"
    cfg.riskThreshold    shouldBe 0.10
    cfg.startingOffsets  shouldBe "latest"
    cfg.triggerInterval  shouldBe "10 seconds"
    cfg.sparkMaster      shouldBe "local[*]"
    cfg.jdbcUrl          shouldBe "jdbc:postgresql://localhost:5432/ridesim"
    cfg.jdbcUser         shouldBe "postgres"
    cfg.jdbcPassword     shouldBe ""
  }

  it should "read overrides from env map" in {
    val cfg = AppConfig.fromEnv(Map(
      "KAFKA_BROKERS"     -> "broker1:9092,broker2:9092",
      "TOPIC_GEOLOCATION" -> "geo.v2",
      "TOPIC_LIFECYCLE"   -> "lifecycle.v2",
      "TOPIC_ALERT"       -> "alerts.v2",
      "RISK_THRESHOLD"    -> "0.25",
      "STARTING_OFFSETS"  -> "earliest",
      "TRIGGER_INTERVAL"  -> "5 seconds",
      "SPARK_MASTER"      -> "spark://cluster:7077",
      "JDBC_URL"          -> "jdbc:postgresql://db-host:5432/ridesim",
      "JDBC_USER"         -> "appuser",
      "JDBC_PASSWORD"     -> "secret"
    ))
    cfg.kafkaBrokers     shouldBe "broker1:9092,broker2:9092"
    cfg.topicGeolocation shouldBe "geo.v2"
    cfg.topicLifecycle   shouldBe "lifecycle.v2"
    cfg.topicAlert       shouldBe "alerts.v2"
    cfg.riskThreshold    shouldBe 0.25
    cfg.startingOffsets  shouldBe "earliest"
    cfg.triggerInterval  shouldBe "5 seconds"
    cfg.sparkMaster      shouldBe "spark://cluster:7077"
    cfg.jdbcUrl          shouldBe "jdbc:postgresql://db-host:5432/ridesim"
    cfg.jdbcUser         shouldBe "appuser"
    cfg.jdbcPassword     shouldBe "secret"
  }

  it should "parse risk threshold as Double" in {
    val cfg = AppConfig.fromEnv(Map("RISK_THRESHOLD" -> "0.05"))
    cfg.riskThreshold shouldBe 0.05
  }
}
