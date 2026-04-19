package com.ridesim.streaming.logic

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object DangerZoneDetector {

  val DefaultReason = "random_zone_risk_10pct"
  val SchemaVersion = "1.0"

  /** Pure-ish DataFrame transformation: given incoming geolocation events, flag
    * a fraction ~ `threshold` as dangerous-zone alerts. Uses `rand()` per-row.
    *
    * Optional `seed` makes the random column deterministic for unit tests.
    */
  def detectAlerts(
      geoEvents: DataFrame,
      threshold: Double,
      seed: Option[Long] = None
  ): DataFrame = {
    val randCol = seed.map(rand).getOrElse(rand())

    geoEvents
      .withColumn("risk_score", randCol)
      .filter(col("risk_score") < lit(threshold))
      .withColumnRenamed("event_id", "source_event_id")
      .withColumnRenamed("timestamp_utc", "source_timestamp_utc")
      .withColumn("event_id", expr("uuid()"))
      .withColumn("alert_timestamp_utc", current_timestamp().cast(StringType))
      .withColumn("risk_alert", lit(true))
      .withColumn("reason", lit(DefaultReason))
      .withColumn("schema_version", lit(SchemaVersion))
  }
}
