package com.ridesim.streaming.logic

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DangerZoneDetectorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("DangerZoneDetectorSpec")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  import spark.implicits._

  private def sampleGeoEvents(n: Int): DataFrame =
    (1 to n).map { i =>
      (
        s"event-$i",
        "2026-04-19T23:00:00Z",
        s"trip-${i % 20}",
        s"user-${i % 50}",
        s"driver-${i % 100}",
        40.0 + i * 0.001,
        -74.0 + i * 0.001,
        "PROGRESS",
        "1.0"
      )
    }.toDF(
      "event_id", "timestamp_utc", "trip_id", "user_id", "driver_id",
      "lat", "lon", "status", "schema_version"
    )

  "detectAlerts" should "produce zero alerts when threshold is 0.0" in {
    val df = sampleGeoEvents(100)
    val alerts = DangerZoneDetector.detectAlerts(df, threshold = 0.0, seed = Some(1L))
    alerts.count() shouldBe 0L
  }

  it should "produce N alerts when threshold is 1.0 (everything is risky)" in {
    val df = sampleGeoEvents(50)
    val alerts = DangerZoneDetector.detectAlerts(df, threshold = 1.0, seed = Some(1L))
    alerts.count() shouldBe 50L
  }

  it should "produce roughly threshold*N alerts for 0.10 (statistical bounds)" in {
    val df = sampleGeoEvents(1000)
    val alerts = DangerZoneDetector.detectAlerts(df, threshold = 0.10, seed = Some(42L))
    val count = alerts.count()

    // Expected ~100; with seed it's deterministic. Allow wide bounds [50, 150].
    count should be >= 50L
    count should be <= 150L
  }

  it should "preserve trip_id / user_id / driver_id correlation" in {
    val df = Seq(
      ("e-xyz", "2026-04-19T23:00:00Z", "trip-42", "user-7", "driver-13",
       4.71, -74.07, "PROGRESS", "1.0")
    ).toDF(
      "event_id", "timestamp_utc", "trip_id", "user_id", "driver_id",
      "lat", "lon", "status", "schema_version"
    )

    val alerts = DangerZoneDetector.detectAlerts(df, threshold = 1.0, seed = Some(7L))
    val row = alerts.collect().head

    row.getAs[String]("trip_id") shouldBe "trip-42"
    row.getAs[String]("user_id") shouldBe "user-7"
    row.getAs[String]("driver_id") shouldBe "driver-13"
    row.getAs[String]("source_event_id") shouldBe "e-xyz"
    row.getAs[String]("source_timestamp_utc") shouldBe "2026-04-19T23:00:00Z"
  }

  it should "generate a new event_id different from the source event_id" in {
    val df = Seq(
      ("e-original", "2026-04-19T23:00:00Z", "trip-1", "user-1", "driver-1",
       0.0, 0.0, "PROGRESS", "1.0")
    ).toDF(
      "event_id", "timestamp_utc", "trip_id", "user_id", "driver_id",
      "lat", "lon", "status", "schema_version"
    )

    val alerts = DangerZoneDetector.detectAlerts(df, threshold = 1.0, seed = Some(2L))
    val row = alerts.collect().head

    val newId = row.getAs[String]("event_id")
    val srcId = row.getAs[String]("source_event_id")

    newId should not be empty
    newId should not equal srcId
    srcId shouldBe "e-original"
  }

  it should "tag every alert row with risk_alert=true and the default reason" in {
    val df = sampleGeoEvents(20)
    val alerts = DangerZoneDetector.detectAlerts(df, threshold = 1.0, seed = Some(3L))

    val allTrue = alerts.filter($"risk_alert" === true).count()
    allTrue shouldBe 20L

    val reasons = alerts.select("reason").distinct().as[String].collect()
    reasons should contain only DangerZoneDetector.DefaultReason
  }

  it should "produce risk_score between 0.0 and 1.0" in {
    val df = sampleGeoEvents(100)
    val alerts = DangerZoneDetector.detectAlerts(df, threshold = 1.0, seed = Some(4L))

    val scores = alerts.select("risk_score").as[Double].collect()
    scores.forall(s => s >= 0.0 && s <= 1.0) shouldBe true
  }

  it should "include an alert_timestamp_utc column" in {
    val df = sampleGeoEvents(5)
    val alerts = DangerZoneDetector.detectAlerts(df, threshold = 1.0, seed = Some(5L))
    alerts.columns should contain("alert_timestamp_utc")
    val nonEmpty = alerts.select("alert_timestamp_utc").as[String].collect().forall(_.nonEmpty)
    nonEmpty shouldBe true
  }
}
