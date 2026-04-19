package com.ridesim.streaming.domain

import org.apache.spark.sql.types._

object Schemas {

  val GeolocationSchema: StructType = StructType(Seq(
    StructField("event_id",       StringType, nullable = true),
    StructField("timestamp_utc",  StringType, nullable = true),
    StructField("trip_id",        StringType, nullable = true),
    StructField("user_id",        StringType, nullable = true),
    StructField("driver_id",      StringType, nullable = true),
    StructField("lat",            DoubleType, nullable = true),
    StructField("lon",            DoubleType, nullable = true),
    StructField("status",         StringType, nullable = true),
    StructField("schema_version", StringType, nullable = true)
  ))
}
