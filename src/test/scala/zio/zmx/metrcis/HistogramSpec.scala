package zio.zmx.metrics

import zio._
import zio.clock.Clock
import zio.duration._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test.environment.TestClock
import zio.zmx._
import zio.zmx.internal._
import zio.zmx.state.MetricType
import zio.zmx.state.DoubleHistogramBuckets

object HistogramSpec extends DefaultRunnableSpec with Generators {

  override def spec = suite("A ZMX Histogram should")(
    startFromZero,
    observe,
    observeDurations
  ) @@ timed @@ timeoutWarning(60.seconds) @@ parallel

  private val startFromZero = test("start from Zero") {
    val key = MetricKey.Histogram("fromZero", DoubleHistogramBuckets.linear(0d, 10d, 11).boundaries)
    metricState.getHistogram(key)

    checkHistogram(key, key.boundaries.map((_, 0L)), 0d, 0L)
  }

  private val observe = testM("observe correctly") {
    val key    = MetricKey.Histogram("increment", DoubleHistogramBuckets.linear(0d, 10d, 11).boundaries)
    val aspect = MetricAspect.observeHistogram("increment", DoubleHistogramBuckets.linear(0d, 10d, 11).boundaries)
    for {
      _ <- ZIO.succeed(50d) @@ aspect
    } yield checkHistogram(key, key.boundaries.map(v => (v, if (v >= 50d) 1L else 0L)), 50d, 1L)
  }

  private val observeDurations: ZSpec[TestClock with Clock, Nothing] = testM("observe durations") {
    val keySuccess = MetricKey.Histogram("success", DoubleHistogramBuckets.linear(0d, 10d, 11).boundaries)
    val keyFailure = MetricKey.Histogram("failure", DoubleHistogramBuckets.linear(0d, 10d, 11).boundaries)
    val measureSuccess = MetricAspect.observeDurations("success", DoubleHistogramBuckets.linear(0d, 10d, 11).boundaries)(_.toMillis.toDouble)
    val measureFailure = MetricAspect.observeFailureDurations("failure", DoubleHistogramBuckets.linear(0d, 10d, 11).boundaries)(_.toMillis.toDouble)
    for {
      success <- (ZIO.succeed(()).delay(Duration.fromMillis(5)) @@ measureSuccess @@ measureFailure).fork
      failure <- (ZIO.fail("Boom").delay(Duration.fromMillis(5)) @@ measureFailure @@ measureSuccess).fork
      _ <- TestClock.adjust(Duration.fromMillis(5))
      _ <- success.join
      _ <- failure.join.flip
    } yield assertCount(keySuccess, 1) && assertCount(keyFailure, 1)
  }

  private def assertCount(key: MetricKey.Histogram, expected: Long) = snapshot()(key).details match {
    case MetricType.DoubleHistogram(_, count, _) => assert(count)(equalTo(expected))
    case _ => assert(())(nothing)
  }

  private def checkHistogram(
    key: MetricKey.Histogram,
    expectedCounts: Chunk[(Double, Long)],
    expectedSum: Double,
    expectedCount: Long
  ) = {
    val optHist   = snapshot().get(key)
    val isCorrect = optHist.get.details match {
      case h @ MetricType.DoubleHistogram(_, _, _) =>
        h.buckets.equals(expectedCounts) &&
          h.sum == expectedSum &&
          h.count == expectedCount
      case _                                       => false

    }
    assert(optHist)(isSome) && assert(isCorrect)(isTrue)
  }
}
