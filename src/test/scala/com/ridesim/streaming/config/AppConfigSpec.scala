package com.ridesim.streaming.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AppConfigSpec extends AnyFlatSpec with Matchers {

  "AppConfig.fromEnv" should "return all defaults when env is empty" in {
    val cfg = AppConfig.fromEnv(Map.empty)
    cfg.kafkaBrokers     shouldBe "localhost:9092"
    cfg.topicGeolocation shouldBe "topic.geolocation"
    cfg.topicAlert       shouldBe "topic.alert"
    cfg.riskThreshold    shouldBe 0.10
    cfg.startingOffsets  shouldBe "latest"
    cfg.triggerInterval  shouldBe "10 seconds"
    cfg.sparkMaster      shouldBe "local[*]"
  }

  it should "read overrides from env map" in {
    val cfg = AppConfig.fromEnv(Map(
      "KAFKA_BROKERS"     -> "broker1:9092,broker2:9092",
      "TOPIC_GEOLOCATION" -> "geo.v2",
      "TOPIC_ALERT"       -> "alerts.v2",
      "RISK_THRESHOLD"    -> "0.25",
      "STARTING_OFFSETS"  -> "earliest",
      "TRIGGER_INTERVAL"  -> "5 seconds",
      "SPARK_MASTER"      -> "spark://cluster:7077"
    ))
    cfg.kafkaBrokers     shouldBe "broker1:9092,broker2:9092"
    cfg.topicGeolocation shouldBe "geo.v2"
    cfg.topicAlert       shouldBe "alerts.v2"
    cfg.riskThreshold    shouldBe 0.25
    cfg.startingOffsets  shouldBe "earliest"
    cfg.triggerInterval  shouldBe "5 seconds"
    cfg.sparkMaster      shouldBe "spark://cluster:7077"
  }

  it should "parse risk threshold as Double" in {
    val cfg = AppConfig.fromEnv(Map("RISK_THRESHOLD" -> "0.05"))
    cfg.riskThreshold shouldBe 0.05
  }
}
