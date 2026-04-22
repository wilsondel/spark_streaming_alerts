package com.ridesim.streaming.pipeline

import com.ridesim.streaming.config.AppConfig
import com.ridesim.streaming.domain.Schemas
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object GeoLocationDbPipeline {

  def run(spark: SparkSession, cfg: AppConfig): StreamingQuery = {
    import spark.implicits._

    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.kafkaBrokers)
      .option("subscribe", cfg.topicGeolocation)
      .option("startingOffsets", cfg.startingOffsets)
      .option("failOnDataLoss", "false")
      .load()

    val parsed = rawStream
      .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
      .select(from_json($"json_str", Schemas.GeolocationSchema).as("geo"), $"kafka_ts")
      .select($"geo.*", $"kafka_ts")

    parsed.writeStream
      .foreachBatch { (batchDf: DataFrame, _: Long) =>
        if (!batchDf.isEmpty)
          batchDf.write
            .format("jdbc")
            .option("url", cfg.jdbcUrl)
            .option("dbtable", "geo_events")
            .option("user", cfg.jdbcUser)
            .option("password", cfg.jdbcPassword)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
      }
      .option("checkpointLocation", s"${cfg.checkpointDir}/geo_db")
      .trigger(Trigger.ProcessingTime(cfg.triggerInterval))
      .start()
  }
}
