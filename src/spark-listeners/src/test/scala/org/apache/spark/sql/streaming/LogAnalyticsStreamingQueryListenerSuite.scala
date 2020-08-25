package org.apache.spark.sql.streaming

import java.util.UUID

import org.apache.spark.listeners.ListenerSuite
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.scalatest.BeforeAndAfterEach

import scala.collection.JavaConversions.mapAsJavaMap

object LogAnalyticsStreamingQueryListenerSuite {
  val queryStartedEvent = new QueryStartedEvent(UUID.randomUUID, UUID.randomUUID, "name", ListenerSuite.EPOCH_TIME_AS_ISO8601)
  val queryTerminatedEvent = new QueryTerminatedEvent(UUID.randomUUID, UUID.randomUUID, None)
  val queryProgressEvent = new QueryProgressEvent(
    new StreamingQueryProgress(
      UUID.randomUUID, //id: java.util.UUID
      UUID.randomUUID, //runId: java.util.UUID
      null, //name: String
      ListenerSuite.EPOCH_TIME_AS_ISO8601, //timestamp: String
      2L, //batchId: Long
      0L, //batchDuration: Long
      mapAsJavaMap(Map("total" -> 0L)), //durationMs: java.util.Map[String,Long]
      mapAsJavaMap(Map.empty[String, String]), //eventTime: java.util.Map[String,String]
      Array(new StateOperatorProgress(
        0, 1, 2)), //stateOperators: Array[org.apache.spark.sql.streaming.StateOperatorProgress]
      Array(
        new SourceProgress(
          "source",
          "123",
          "456",
          678,
          Double.NaN,
          Double.NegativeInfinity
        )
      ), //sources: Array[org.apache.spark.sql.streaming.SourceProgress]
      new SinkProgress("sink"), //sink: org.apache.spark.sql.streaming.SinkProgress
      null //observedMetrics: java.util.Map[String,org.apache.spark.sql.Row])org.apache.spark.sql.streaming.StreamingQueryProgress
    )
  )
}

class LogAnalyticsStreamingQueryListenerSuite extends ListenerSuite
  with BeforeAndAfterEach {

  test("should invoke sendToSink for QueryStartedEvent with full class name") {
    val (json, event) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryStartedEvent
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for QueryTerminatedEvent with full class name") {
    val (json, event) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryTerminatedEvent
    )

    this.assertEvent(json, event)
  }

  test("should invoke sendToSink for QueryProgressEvent with full class name") {
    val (json, event) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryProgressEvent
    )

    this.assertEvent(json, event)
  }

  test("QueryProgressEvent should have expected SparkEventTime") {
    val (json, _) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryProgressEvent
    )

    this.assertSparkEventTime(
      json,
      (_, value) => assert(value.extract[String] === ListenerSuite.EPOCH_TIME_AS_ISO8601)
    )
  }

  test("QueryStartedEvent should have SparkEventTime") {
    val (json, _) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryStartedEvent
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }

  test("QueryTerminatedEvent should have SparkEventTime") {
    val (json, _) = this.onStreamingQueryListenerEvent(
      LogAnalyticsStreamingQueryListenerSuite.queryTerminatedEvent
    )
    this.assertSparkEventTime(
      json,
      (_, value) => assert(!value.extract[String].isEmpty)
    )
  }
}
