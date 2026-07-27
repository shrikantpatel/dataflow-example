package com.example.dataflow;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Third learning example: side inputs and side outputs (multiple outputs).
 *
 * <p><b>Side input</b>: a small "stopwords" file is read into a {@link PCollectionView} (via
 * {@link View#asList()}) and broadcast to every worker. Inside the main {@link DoFn}, it's read
 * with {@code c.sideInput(view)} to filter out common words — this is how you "enrich" or filter
 * a main PCollection using data from another, smaller PCollection, without a join/shuffle.
 *
 * <p><b>Side outputs</b>: rather than emitting exactly one output per element, the same {@link
 * DoFn} routes each surviving word to one of two additional outputs — "short words" (length &lt;=
 * 4) or "long words" (length &gt;= 8) — using {@link TupleTag}s and {@code
 * ParDo.of(fn).withOutputTags(...)}. The main output and each side output become independent
 * {@link PCollection}s that can be processed/written differently.
 *
 * <p>Run it locally:
 *
 * <pre>{@code
 * ./gradlew run -PmainClass=com.example.dataflow.WordEnrichment \
 *     --args="--inputFile=src/main/resources/input.txt \
 *             --stopWordsFile=src/main/resources/stopwords.txt \
 *             --output=build/output/enrichment"
 * }</pre>
 *
 * <p>This produces three sets of output files: {@code enrichment-filtered-*}, {@code
 * enrichment-short-*}, and {@code enrichment-long-*}.
 */
public class WordEnrichment {

  // Tags identify each output PCollection produced by the ParDo below. The main output uses the
  // return value of withOutputTags(); side outputs are looked up from the resulting tuple by tag.
  // Package-private (not private) so tests can reuse these exact tag instances rather than
  // creating new ones - TupleTag identity matters when wiring them via withOutputTags().
  static final TupleTag<String> FILTERED_WORDS = new TupleTag<String>() {};
  static final TupleTag<String> SHORT_WORDS = new TupleTag<String>() {};
  static final TupleTag<String> LONG_WORDS = new TupleTag<String>() {};

  private static final int SHORT_WORD_MAX_LENGTH = 4;
  private static final int LONG_WORD_MIN_LENGTH = 8;

  /** Splits lines into words, drops stopwords (via side input), and routes by word length. */
  static class FilterAndClassifyFn extends DoFn<String, String> {
    private final PCollectionView<List<String>> stopWordsView;

    FilterAndClassifyFn(PCollectionView<List<String>> stopWordsView) {
      this.stopWordsView = stopWordsView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      // Read the side input: the same small list of stopwords, available on every worker.
      Set<String> stopWords = new HashSet<>(c.sideInput(stopWordsView));

      for (String rawWord : c.element().split("[^\\p{L}]+")) {
        if (rawWord.isEmpty()) {
          continue;
        }
        String word = rawWord.toLowerCase();
        if (stopWords.contains(word)) {
          continue;
        }

        // Main output: every non-stopword.
        c.output(word);

        // Side outputs: additionally classify by length.
        if (word.length() <= SHORT_WORD_MAX_LENGTH) {
          c.output(SHORT_WORDS, word);
        } else if (word.length() >= LONG_WORD_MIN_LENGTH) {
          c.output(LONG_WORDS, word);
        }
      }
    }
  }

  /** Custom pipeline options for this example. */
  public interface WordEnrichmentOptions extends PipelineOptions {
    @Description("Path to the input text file to read")
    @Default.String("src/main/resources/input.txt")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path to a file containing one stopword per line")
    @Default.String("src/main/resources/stopwords.txt")
    String getStopWordsFile();

    void setStopWordsFile(String value);

    @Description("Path prefix for the output file(s)")
    @Default.String("build/output/enrichment")
    String getOutput();

    void setOutput(String value);
  }

  public static void main(String[] args) {
    WordEnrichmentOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WordEnrichmentOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    // Build the side input: a small PCollection of stopwords, materialized as a List and
    // broadcast to every worker processing the main PCollection.
    PCollectionView<List<String>> stopWordsView =
        pipeline
            .apply("ReadStopWords", TextIO.read().from(options.getStopWordsFile()))
            .apply("View.AsList", View.asList());

    PCollection<String> lines =
        pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()));

    PCollectionTuple results =
        lines.apply(
            "FilterAndClassify",
            ParDo.of(new FilterAndClassifyFn(stopWordsView))
                .withSideInputs(stopWordsView)
                .withOutputTags(FILTERED_WORDS, TupleTagList.of(SHORT_WORDS).and(LONG_WORDS)));

    results.get(FILTERED_WORDS)
        .apply("WriteFiltered", TextIO.write().to(options.getOutput() + "-filtered").withSuffix(".txt"));
    results.get(SHORT_WORDS)
        .apply("WriteShort", TextIO.write().to(options.getOutput() + "-short").withSuffix(".txt"));
    results.get(LONG_WORDS)
        .apply("WriteLong", TextIO.write().to(options.getOutput() + "-long").withSuffix(".txt"));

    pipeline.run().waitUntilFinish();
  }
}
