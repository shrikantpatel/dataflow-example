package com.example.dataflow;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for the {@link WordLengthStats} custom Combine transform. */
public class WordLengthStatsTest {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  // Words starting with 'a': "Apache" (6), "and" (3) -> average 4.5
  // Words starting with 'b': "Beam" (4), "batch" (5) -> average 4.5
  private static final List<String> LINES = Arrays.asList("Apache and", "Beam batch");

  @Test
  public void averageFnComputesAveragePerKey() {
    PCollection<KV<String, Integer>> keyedLengths =
        pipeline
            .apply(Create.of(LINES))
            .apply(ParDo.of(new WordLengthStats.ExtractFirstLetterAndLengthFn()));

    PCollection<KV<String, Double>> averages =
        keyedLengths.apply(Combine.perKey(new WordLengthStats.AverageFn()));

    PAssert.that(averages).containsInAnyOrder(KV.of("a", 4.5), KV.of("b", 4.5));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void averageFnHandlesMergingAccumulators() {
    WordLengthStats.AverageFn fn = new WordLengthStats.AverageFn();
    WordLengthStats.AverageAccumulator acc1 = fn.createAccumulator();
    acc1 = fn.addInput(acc1, 2);
    acc1 = fn.addInput(acc1, 4);

    WordLengthStats.AverageAccumulator acc2 = fn.createAccumulator();
    acc2 = fn.addInput(acc2, 9);

    WordLengthStats.AverageAccumulator merged = fn.mergeAccumulators(Arrays.asList(acc1, acc2));

    // (2 + 4 + 9) / 3 = 5.0
    org.junit.Assert.assertEquals(5.0, fn.extractOutput(merged), 0.0001);
  }
}
