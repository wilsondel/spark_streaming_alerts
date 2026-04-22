package com.ridesim.streaming.pipeline

import com.ridesim.streaming.config.AppConfig
import com.ridesim.streaming.domain.Schemas
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object LifecyclePipeline {

  def run(spark: SparkSession, cfg: AppConfig): StreamingQuery = {
    import spark.implicits._

    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.kafkaBrokers)
      .option("subscribe", cfg.topicLifecycle)
      .option("startingOffsets", cfg.startingOffsets)
      .option("failOnDataLoss", "false")
      .load()

    val parsed = rawStream
      .selectExpr("CAST(value AS STRING) AS json_str")
      .select(from_json($"json_str", Schemas.LifecycleSchema).as("lc"))
      .select(
        $"lc.event_id",
        $"lc.event_type",
        $"lc.timestamp_utc",
        $"lc.trip_id",
        $"lc.user_id",
        $"lc.driver_id",
        $"lc.ride_type",
        $"lc.status",
        $"lc.origin.lat".as("origin_lat"),
        $"lc.origin.lon".as("origin_lon"),
        $"lc.origin.city".as("origin_city"),
        $"lc.origin.country".as("origin_country"),
        $"lc.destination.lat".as("destination_lat"),
        $"lc.destination.lon".as("destination_lon"),
        $"lc.destination.city".as("destination_city"),
        $"lc.destination.country".as("destination_country"),
        $"lc.estimated_price",
        $"lc.currency",
        $"lc.schema_version"
      )

    parsed.writeStream
      .foreachBatch { (batchDf: DataFrame, _: Long) =>
        if (!batchDf.isEmpty)
          batchDf.write
            .format("jdbc")
            .option("url", cfg.jdbcUrl)
            .option("dbtable", "lifecycle_events")
            .option("user", cfg.jdbcUser)
            .option("password", cfg.jdbcPassword)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
      }
      .option("checkpointLocation", s"${cfg.checkpointDir}/lifecycle")
      .trigger(Trigger.ProcessingTime(cfg.triggerInterval))
      .start()
  }
}
