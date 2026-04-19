package com.ridesim.streaming.config

final case class AppConfig(
    kafkaBrokers: String,
    topicGeolocation: String,
    topicAlert: String,
    checkpointDir: String,
    riskThreshold: Double,
    startingOffsets: String,
    triggerInterval: String,
    appName: String,
    sparkMaster: String
)

object AppConfig {

  def fromEnv(env: Map[String, String] = sys.env): AppConfig =
    AppConfig(
      kafkaBrokers     = env.getOrElse("KAFKA_BROKERS",       "localhost:9092"),
      topicGeolocation = env.getOrElse("TOPIC_GEOLOCATION",   "topic.geolocation"),
      topicAlert       = env.getOrElse("TOPIC_ALERT",         "topic.alert"),
      checkpointDir    = env.getOrElse("CHECKPOINT_DIR",      "/tmp/spark-checkpoints/alerts"),
      riskThreshold    = env.getOrElse("RISK_THRESHOLD",      "0.10").toDouble,
      startingOffsets  = env.getOrElse("STARTING_OFFSETS",    "latest"),
      triggerInterval  = env.getOrElse("TRIGGER_INTERVAL",    "10 seconds"),
      appName          = env.getOrElse("APP_NAME",            "danger-zone-streaming"),
      sparkMaster      = env.getOrElse("SPARK_MASTER",        "local[*]")
    )
}
