package com.ridesim.streaming

import com.ridesim.streaming.config.AppConfig
import com.ridesim.streaming.pipeline.AlertPipeline
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

    val query = AlertPipeline.run(spark, cfg)
    log.info(s"streaming_query_started id=${query.id} name=${query.name}")

    sys.addShutdownHook {
      log.info("shutdown_hook_received_stopping_query")
      try query.stop() catch { case _: Throwable => () }
      try spark.stop() catch { case _: Throwable => () }
    }

    query.awaitTermination()
  }
}
