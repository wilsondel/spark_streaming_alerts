package com.ridesim.streaming

import com.ridesim.streaming.config.AppConfig
import com.ridesim.streaming.pipeline.{AlertPipeline, GeoLocationDbPipeline, LifecyclePipeline}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val cfg = AppConfig.fromEnv()
    log.info(s"starting_app config=$cfg")

    val spark = SparkSession.builder()
      .appName(cfg.appName)
      .master(cfg.sparkMaster)
      .config("spark.sql.streaming.checkpointLocation", cfg.checkpointDir)
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val alertQuery    = AlertPipeline.run(spark, cfg)
    val lifecycleQuery = LifecyclePipeline.run(spark, cfg)
    val geoDbQuery    = GeoLocationDbPipeline.run(spark, cfg)

    log.info(s"alert_query_started id=${alertQuery.id}")
    log.info(s"lifecycle_query_started id=${lifecycleQuery.id}")
    log.info(s"geo_db_query_started id=${geoDbQuery.id}")

    sys.addShutdownHook {
      log.info("shutdown_hook_received_stopping_queries")
      Seq(alertQuery, lifecycleQuery, geoDbQuery).foreach { q =>
        try q.stop() catch { case _: Throwable => () }
      }
      try spark.stop() catch { case _: Throwable => () }
    }

    spark.streams.awaitAnyTermination()
  }
}
