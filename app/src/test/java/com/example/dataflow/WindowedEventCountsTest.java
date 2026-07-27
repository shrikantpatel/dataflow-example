package com.example.dataflow;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/**
 * Verifies that {@link WindowedEventCounts}'s windowing/trigger configuration produces an
 * ON_TIME pane for a window, and a separate, later LATE pane once a late element arrives for that
 * same (already-closed) window.
 */
public class WindowedEventCountsTest {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private static final Duration WINDOW_SIZE = Duration.standardSeconds(10);

  @Test
  public void onTimeAndLatePanesFireSeparatelyForSameWindow() {
    pipeline.getOptions().as(StreamingOptions.class).setStreaming(true);

    Instant start = Instant.EPOCH;
    IntervalWindow firstWindow = new IntervalWindow(start, start.plus(WINDOW_SIZE));

    TestStream<String> stream =
        TestStream.create(StringUtf8Coder.of())
            .addElements(
                TimestampedValue.of("page-a", start.plus(Duration.standardSeconds(1))),
                TimestampedValue.of("page-a", start.plus(Duration.standardSeconds(4))))
            .advanceWatermarkTo(start.plus(WINDOW_SIZE))
            .addElements(TimestampedValue.of("page-a", start.plus(Duration.standardSeconds(5))))
            .advanceWatermarkToInfinity();

    PCollection<KV<String, Long>> counts =
        pipeline
            .apply(stream)
            .apply(
                Window.<String>into(FixedWindows.of(WINDOW_SIZE))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withLateFirings(AfterPane.elementCountAtLeast(1)))
                    .withAllowedLateness(Duration.standardSeconds(30))
                    .accumulatingFiredPanes())
            .apply(Count.perElement());

    // The ON_TIME pane fires once the watermark passes the window, containing only the 2
    // elements seen so far.
    PAssert.that(counts).inOnTimePane(firstWindow).containsInAnyOrder(KV.of("page-a", 2L));

    // Because accumulatingFiredPanes() is used, the LATE pane contains the running total: all 3
    // elements, including the late arrival.
    PAssert.that(counts).inLatePane(firstWindow).containsInAnyOrder(KV.of("page-a", 3L));

    pipeline.run().waitUntilFinish();
  }
}
