package com.ridesim.streaming.pipeline

import com.ridesim.streaming.config.AppConfig
import com.ridesim.streaming.domain.Schemas
import com.ridesim.streaming.logic.DangerZoneDetector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object AlertPipeline {

  def run(spark: SparkSession, cfg: AppConfig): StreamingQuery = {
    import spark.implicits._

    val geoStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.kafkaBrokers)
      .option("subscribe", cfg.topicGeolocation)
      .option("startingOffsets", cfg.startingOffsets)
      .option("failOnDataLoss", "false")
      .load()

    val parsed = geoStream
      .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
      .select(from_json($"json_str", Schemas.GeolocationSchema).as("geo"), $"kafka_ts")
      .select($"geo.*", $"kafka_ts")

    val alerts = DangerZoneDetector.detectAlerts(parsed, cfg.riskThreshold)

    val output = alerts.select(
      $"trip_id".cast("string").as("key"),
      to_json(struct(
        $"event_id",
        $"source_event_id",
        $"source_timestamp_utc",
        $"alert_timestamp_utc",
        $"trip_id",
        $"user_id",
        $"driver_id",
        $"lat",
        $"lon",
        $"risk_alert",
        $"risk_score",
        $"reason",
        $"schema_version"
      )).as("value")
    )

    output.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.kafkaBrokers)
      .option("topic", cfg.topicAlert)
      .option("checkpointLocation", cfg.checkpointDir)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(cfg.triggerInterval))
      .start()
  }
}
