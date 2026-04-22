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

  private val LocationSchema: StructType = StructType(Seq(
    StructField("lat",     DoubleType, nullable = true),
    StructField("lon",     DoubleType, nullable = true),
    StructField("city",    StringType, nullable = true),
    StructField("country", StringType, nullable = true)
  ))

  val LifecycleSchema: StructType = StructType(Seq(
    StructField("event_id",        StringType,    nullable = true),
    StructField("event_type",      StringType,    nullable = true),
    StructField("timestamp_utc",   StringType,    nullable = true),
    StructField("trip_id",         StringType,    nullable = true),
    StructField("user_id",         StringType,    nullable = true),
    StructField("driver_id",       StringType,    nullable = true),
    StructField("ride_type",       StringType,    nullable = true),
    StructField("status",          StringType,    nullable = true),
    StructField("origin",          LocationSchema, nullable = true),
    StructField("destination",     LocationSchema, nullable = true),
    StructField("estimated_price", DoubleType,    nullable = true),
    StructField("currency",        StringType,    nullable = true),
    StructField("schema_version",  StringType,    nullable = true)
  ))
}
