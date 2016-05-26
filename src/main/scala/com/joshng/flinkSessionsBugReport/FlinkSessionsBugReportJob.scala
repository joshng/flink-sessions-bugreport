package com.joshng.flinkSessionsBugReport

import java.util.concurrent.atomic.AtomicInteger

import com.google.common.collect.Maps
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object FlinkSessionsBugReportJob {

  val sessionWindowGap = Time.minutes(5)
  val timeWindowDuration = Time.hours(1)
  val sessionCount = 20000
  val maxEventsPerSession = 5
  val aggregateCardinality = 8 // count of distinct aggregate-ids to generate

  val maxSessionTimestamp = Time.days(2).toMilliseconds.toInt.ensuring(_ > 0) // guard against overflow for large values

  val sessionWindowGapMillis = sessionWindowGap.toMilliseconds.toInt.ensuring(_ > 0)
  val maxTimeBetweenEvents = sessionWindowGapMillis * 2 / 3 // ensure all events arrive in time to be windowed
  val maxArrivalLatency = sessionWindowGapMillis - maxTimeBetweenEvents // ensure all events arrive in time to be windowed

  val sessionIdCounter = new AtomicInteger
  val rand = new Random(0)

  def main(args: Array[String]) {
    val flink = StreamExecutionEnvironment.getExecutionEnvironment
    flink.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val adjustTimestamps = args match {
      case Array() => true
      case Array("-n") => false
      case _ => throw new IllegalArgumentException(s"What? Use '-n' to suppress timestamp reassignment. Unrecognized argument(s): '${args.mkString(" ")}'}")
    }

    val events = generateTestData()

    val sessionized: DataStream[TimestampedStats] = flink.fromCollection(events)
            .assignTimestampsAndWatermarks(new TimestampedWatermarkAssigner[Event](sessionWindowGap))
            .keyBy(_.sessionId)
            .window(EventTimeSessionWindows.withGap(sessionWindowGap))
            .apply { (sessionId, window, sessionEvents, collector: Collector[TimestampedStats]) =>
              val timestamp = sessionEvents.map(_.timestamp).max
              val eventCount = sessionEvents.size
              collector.collect(TimestampedStats(timestamp, Stats(sessionId.aggregateId, eventCount)))
            }

    val adjusted: DataStream[TimestampedStats] = if (adjustTimestamps) {
      // *****  BUG: this causes the sessions to be fragmented incorrectly  *****
      sessionized.assignTimestampsAndWatermarks(new TimestampedWatermarkAssigner[TimestampedStats](sessionWindowGap))
    } else {
      sessionized
    }

    val hourlyAggregates = adjusted.map(_.stats)
            .keyBy(_.aggregateId)
            .window(TumblingEventTimeWindows.of(timeWindowDuration))
            .apply(
              _+_,
              (aggregateId, window, stats, collector: Collector[TimestampedStats]) =>
                collector.collect(TimestampedStats(window.getStart, stats.reduce(_+_)))
            )

    hourlyAggregates.addSink(OutputCollector.collect _)

    flink.execute()

    import collection.JavaConverters._
    val expected: Map[OutputId, Integer] = computeOutputExpectations(events, adjustTimestamps)
    val results = OutputCollector.resultBuffer

    val expectedSum = expected.values.map(_.toInt).sum
    val actualSum = results.map(_.stats.eventCount).sum

    // confirm all events are counted
    assert(expectedSum == events.size)
    assert(actualSum == expectedSum, s"Expected total event-count $expectedSum, actual $actualSum")

    assert(results.size == expected.size, s"Expected ${expected.size} outputs, but found ${results.size}")
    val actual: Map[OutputId, Integer] = results.map(_.byOutputId).toMap

    // confirm aggregate-counts are as expected
    assert(expected.size == actual.size, s"Result-count mismatch: expected ${expected.size}, actual ${actual.size}")

    val difference = Maps.difference[OutputId, Integer](actual.asJava, expected.asJava)

    assert(difference.areEqual, difference)
  }

  def generateTestData(): Seq[Event] = {
    val events: Seq[Event] = for {
      _ <- 1 until sessionCount
      sessionId = SessionId(sessionIdCounter.incrementAndGet)
      sessionTimestamp = rand.nextInt(maxSessionTimestamp)
      sessionEventCount = rand1ToN(maxEventsPerSession)
      delay <- delayStream(maxTimeBetweenEvents).take(sessionEventCount)
    } yield Event(sessionTimestamp + delay, sessionId)

    // shift each event by a random latency to simulate out-of-order delivery
    val permuted = events.zip(Stream.continually(rand.nextInt(maxArrivalLatency)))
            .sortBy { case (event, latency) => event.timestamp + latency }
            .map(_._1)

    permuted
  }

  def computeOutputExpectations(events: Seq[Event], adjustTimestamps: Boolean): Map[OutputId, Integer] = {
    val timestampMassager: (Long => Long) = if (adjustTimestamps) {
      // window timestamps will be adjusted to match the last timestamp in each session
      identity
    } else {
      // window timestamps will be offset by the gap
      _ + sessionWindowGapMillis - 1
    }

    val windowMillis = timeWindowDuration.toMilliseconds

    val sessions: Map[SessionId, Seq[Event]] = events.groupBy(_.sessionId)
    val countsByOutputId: Iterable[(OutputId, Int)] = sessions.view.map { case (sessionId, sessionEvents) =>
      val lastTimestamp = sessionEvents.iterator.map(_.timestamp).max
      val expectedTimestamp = timestampMassager(lastTimestamp)
      OutputId(sessionId.aggregateId, expectedTimestamp.floor(windowMillis)) -> sessionEvents.size
    }
    countsByOutputId.groupBy(_._1) // group by OutputId
            .mapValues(iterableView => new java.lang.Integer(iterableView.map(_._2).sum)) // sum counts by OutputId
            .map(identity) // mapValues is lazy; make a strict copy
  }

  def rand1ToN(n: Int) = rand.nextInt(n - 1) + 1

  def delayStream(maxDelay: Int): Stream[Int] = Stream.iterate(0)(_ + rand1ToN(maxDelay))

  implicit class RichLong(n: Long) {
    def floor(window: Long) = n - n % window
  }
}

case class SessionId(id: Long) {
  // artificially assign to an aggregate based on session-id
  def aggregateId = AggregateId(id % FlinkSessionsBugReportJob.aggregateCardinality)
}

case class AggregateId(id: Long)

trait Timestamped {
  def timestamp: Long
}

case class Event(timestamp: Long, sessionId: SessionId) extends Timestamped

case class Stats(aggregateId: AggregateId, eventCount: Int) {
  def +(other: Stats) = copy(eventCount = this.eventCount + other.eventCount)
}

case class TimestampedStats(timestamp: Long, stats: Stats) extends Timestamped {
  def byOutputId: (OutputId, Integer) = OutputId(stats.aggregateId, timestamp) -> stats.eventCount
}

case class OutputId(aggregateId: AggregateId, timestamp: Long)

case class TimestampedWatermarkAssigner[T <: Timestamped](stragglerDelay: Long) extends AssignerWithPeriodicWatermarks[T] {
  def this(time: Time) = this(time.toMilliseconds)
  @volatile var maxObservedTimestamp = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxObservedTimestamp - stragglerDelay)

  override def extractTimestamp(t: T, l: Long): Long = {
    val timestamp = t.timestamp
    maxObservedTimestamp = maxObservedTimestamp max timestamp
    timestamp
  }
}

object OutputCollector {
  val resultBuffer = ArrayBuffer[TimestampedStats]()
  val seenIds = mutable.Set[OutputId]()

  def collect(result: TimestampedStats): Unit = synchronized {
    // ******* enabling this assert will expose the bug immediately *******
//    assert(seenIds.add(result.unzip._1), s"Redundant OutputId: $result")
    resultBuffer += result
  }
}

