package com.example.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Fourth learning example: event-time windowing and triggers.
 *
 * <p>Earlier examples processed a whole file as one bounded batch. Real streaming pipelines
 * instead group elements by the time they actually happened ("event time") into {@code Window}s,
 * and use {@code Trigger}s to decide *when* to emit results for a window - which matters because
 * data can arrive late or out of order.
 *
 * <p>Since there's no real streaming source (Kafka/PubSub) wired up yet, this example uses Beam's
 * {@link TestStream} - a source built specifically for deterministically simulating an unbounded
 * stream: you control exactly which elements arrive, with what event-time timestamps, and when
 * the watermark (Beam's estimate of "we won't see anything earlier than this again") advances.
 * This is the standard, idiomatic way to learn/demo windowing locally without external infra, and
 * it terminates cleanly once the watermark is advanced to infinity - no manual pipeline
 * cancellation needed.
 *
 * <p>Key concepts:
 *
 * <ul>
 *   <li><b>Fixed windows</b> - non-overlapping, equal-sized time buckets (10s here) that elements
 *       are assigned to based on their event-time timestamp.
 *   <li><b>Watermark</b> - Beam's notion of "complete" event time; a window's default trigger
 *       fires once the watermark passes the window's end.
 *   <li><b>Late data &amp; allowed lateness</b> - an element with a timestamp from an
 *       already-closed window can still be incorporated if it arrives within the window's {@code
 *       allowedLateness}, producing an additional ("late") pane.
 *   <li><b>Accumulation mode</b> - {@code accumulatingFiredPanes()} means each new pane for a
 *       window contains the running total so far (as opposed to {@code discardingFiredPanes()},
 *       where each pane only contains what's new since the last firing).
 * </ul>
 *
 * <p>Run it locally (no extra file arguments needed - the "stream" is simulated in code):
 *
 * <pre>{@code
 * ./gradlew run -PmainClass=com.example.dataflow.WindowedEventCounts
 * }</pre>
 *
 * <p>Watch the console output: you'll see the "page-a" window fire once ON_TIME with count=2, then
 * fire again LATE with count=3 after a late-arriving event is added for the same (already closed)
 * window.
 */
public class WindowedEventCounts {

  private static final Duration WINDOW_SIZE = Duration.standardSeconds(10);
  private static final Duration ALLOWED_LATENESS = Duration.standardSeconds(30);

  /** Formats a windowed (key, count) result, including pane timing, for console output. */
  static class FormatWindowedResultFn extends DoFn<KV<String, Long>, String> {
    @ProcessElement
    public void processElement(
        @Element KV<String, Long> element,
        BoundedWindow window,
        PaneInfo pane,
        OutputReceiver<String> out) {
      out.output(
          String.format(
              "window=%s key=%s count=%d pane=%s (isLast=%s)",
              window,
              element.getKey(),
              element.getValue(),
              pane.getTiming(),
              pane.isLast()));
    }
  }

  /** Builds the simulated event stream: page-view events arriving in and out of order. */
  private static TestStream<String> buildSimulatedStream() {
    Instant start = Instant.EPOCH;
    return TestStream.create(StringUtf8Coder.of())
        // --- Window [0s, 10s) ---
        .addElements(
            TimestampedValue.of("page-a", start.plus(Duration.standardSeconds(1))),
            TimestampedValue.of("page-a", start.plus(Duration.standardSeconds(4))))
        // Advance the watermark past the end of the first window: this triggers its ON_TIME pane.
        .advanceWatermarkTo(start.plus(Duration.standardSeconds(10)))
        // --- Window [10s, 20s) ---
        .addElements(TimestampedValue.of("page-b", start.plus(Duration.standardSeconds(12))))
        .advanceWatermarkTo(start.plus(Duration.standardSeconds(20)))
        // A LATE event for the first window [0s, 10s), which already fired. Because it arrives
        // within ALLOWED_LATENESS of the watermark passing that window's end, Beam still
        // incorporates it and fires an additional LATE pane instead of dropping it.
        .addElements(TimestampedValue.of("page-a", start.plus(Duration.standardSeconds(5))))
        // Signal there is no more data - lets the (streaming) pipeline finish and terminate.
        .advanceWatermarkToInfinity();
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    // TestStream models an unbounded source, so the pipeline must run in streaming mode.
    options.as(StreamingOptions.class).setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> events = pipeline.apply("SimulatedEventStream", buildSimulatedStream());

    events
        .apply(
            "ApplyFixedWindows",
            Window.<String>into(FixedWindows.of(WINDOW_SIZE))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                .withAllowedLateness(ALLOWED_LATENESS)
                .accumulatingFiredPanes())
        .apply("CountPerKey", Count.perElement())
        .apply("FormatResults", ParDo.of(new FormatWindowedResultFn()))
        .apply("PrintResults", ParDo.of(new PrintFn()));

    pipeline.run().waitUntilFinish();
  }

  /** Prints each formatted line to stdout so results are visible when run from the console. */
  static class PrintFn extends DoFn<String, Void> {
    @ProcessElement
    public void processElement(@Element String line) {
      System.out.println(line);
    }
  }
}
