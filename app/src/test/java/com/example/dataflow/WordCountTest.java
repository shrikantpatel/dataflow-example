package com.example.dataflow;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for the {@link WordCount} pipeline transforms, using an in-memory TestPipeline. */
public class WordCountTest {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private static final List<String> LINES =
      Arrays.asList("Apache Beam is great", "Beam Beam Beam");

  @Test
  public void countsWordsCorrectly() {
    PCollection<String> input = pipeline.apply(Create.of(LINES));

    PCollection<String> results =
        input
            .apply(ParDo.of(new WordCount.ExtractWordsFn()))
            .apply(Count.<String>perElement())
            .apply(MapElements.via(new WordCount.FormatAsTextFn()));

    PAssert.that(results)
        .containsInAnyOrder("apache: 1", "beam: 4", "is: 1", "great: 1");

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void countPerElementProducesExpectedCounts() {
    PCollection<KV<String, Long>> counts =
        pipeline
            .apply(Create.of(LINES))
            .apply(ParDo.of(new WordCount.ExtractWordsFn()))
            .apply(Count.perElement());

    PAssert.that(counts).containsInAnyOrder(KV.of("beam", 4L), KV.of("apache", 1L),
        KV.of("is", 1L), KV.of("great", 1L));

    pipeline.run().waitUntilFinish();
  }
}
