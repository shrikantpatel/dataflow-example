package com.example.dataflow;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for {@link WordEnrichment}, exercising both the side input and side outputs. */
public class WordEnrichmentTest {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void filtersStopWordsAndClassifiesByLength() {
    List<String> stopWords = Arrays.asList("is", "a", "the");
    List<String> lines = Arrays.asList("Beam is a great tool", "the pipeline abstraction is powerful");

    PCollectionView<List<String>> stopWordsView =
        pipeline.apply("StopWords", Create.of(stopWords)).apply(View.asList());

    PCollection<String> linesPCollection = pipeline.apply("Lines", Create.of(lines));

    PCollectionTuple results =
        linesPCollection.apply(
            "FilterAndClassify",
            ParDo.of(new WordEnrichment.FilterAndClassifyFn(stopWordsView))
                .withSideInputs(stopWordsView)
                .withOutputTags(
                    WordEnrichment.FILTERED_WORDS,
                    TupleTagList.of(WordEnrichment.SHORT_WORDS).and(WordEnrichment.LONG_WORDS)));

    // "is", "a", "the" removed; everything else lowercased and kept in the main output.
    PAssert.that(results.get(WordEnrichment.FILTERED_WORDS))
        .containsInAnyOrder(
            "beam", "great", "tool", "pipeline", "abstraction", "powerful");

    // Words with length <= 4: beam(4), tool(4)
    PAssert.that(results.get(WordEnrichment.SHORT_WORDS)).containsInAnyOrder("beam", "tool");

    // Words with length >= 8: pipeline(8), abstraction(11), powerful(8)
    PAssert.that(results.get(WordEnrichment.LONG_WORDS))
        .containsInAnyOrder("pipeline", "abstraction", "powerful");

    pipeline.run().waitUntilFinish();
  }
}
