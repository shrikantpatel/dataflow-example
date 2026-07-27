package com.example.dataflow;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Second learning example: Combine transforms.
 *
 * <p>WordCount used the built-in {@code Count.perElement()} transform. This example instead
 * writes a <b>custom {@link CombineFn}</b> to compute the average word length, grouped by the
 * word's first letter. It shows the pattern behind Beam's built-in Combine transforms (Sum, Mean,
 * Top, etc.) and how to write your own.
 *
 * <p>A {@link CombineFn}&lt;InputT, AccumT, OutputT&gt; has 4 required methods:
 *
 * <ul>
 *   <li>{@code createAccumulator()} - creates a fresh, empty accumulator.
 *   <li>{@code addInput(accum, input)} - folds one input element into an accumulator.
 *   <li>{@code mergeAccumulators(accums)} - merges partial accumulators (from different bundles /
 *       workers) into one. This is what lets Combine be distributed and run partially on each
 *       worker before merging, unlike a plain GroupByKey + ParDo.
 *   <li>{@code extractOutput(accum)} - produces the final result from an accumulator.
 * </ul>
 *
 * <p>Run it locally:
 *
 * <pre>{@code
 * ./gradlew run -PmainClass=com.example.dataflow.WordLengthStats \
 *     --args="--inputFile=src/main/resources/input.txt --output=build/output/word-length-stats"
 * }</pre>
 */
public class WordLengthStats {

  /** Mutable accumulator tracking a running sum and count, used to compute an average. */
  static class AverageAccumulator implements Serializable {
    long sum = 0;
    long count = 0;
  }

  /** Custom CombineFn that computes the average of a set of integers (e.g. word lengths). */
  static class AverageFn extends CombineFn<Integer, AverageAccumulator, Double> {
    @Override
    public AverageAccumulator createAccumulator() {
      return new AverageAccumulator();
    }

    @Override
    public AverageAccumulator addInput(AverageAccumulator accumulator, Integer input) {
      accumulator.sum += input;
      accumulator.count += 1;
      return accumulator;
    }

    @Override
    public AverageAccumulator mergeAccumulators(Iterable<AverageAccumulator> accumulators) {
      AverageAccumulator merged = createAccumulator();
      for (AverageAccumulator accumulator : accumulators) {
        merged.sum += accumulator.sum;
        merged.count += accumulator.count;
      }
      return merged;
    }

    @Override
    public Double extractOutput(AverageAccumulator accumulator) {
      return accumulator.count == 0 ? 0.0 : (double) accumulator.sum / accumulator.count;
    }
  }

  /** Splits lines into words and emits KV(firstLetter, wordLength) for each word. */
  static class ExtractFirstLetterAndLengthFn extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<KV<String, Integer>> out) {
      for (String word : line.split("[^\\p{L}]+")) {
        if (!word.isEmpty()) {
          String key = word.substring(0, 1).toLowerCase();
          out.output(KV.of(key, word.length()));
        }
      }
    }
  }

  /** Custom pipeline options: adds --inputFile and --output flags. */
  public interface WordLengthStatsOptions extends PipelineOptions {
    @Description("Path to the input text file to read")
    @Default.String("src/main/resources/input.txt")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path prefix for the output file(s)")
    @Default.String("build/output/word-length-stats")
    String getOutput();

    void setOutput(String value);
  }

  public static void main(String[] args) {
    WordLengthStatsOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WordLengthStatsOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<String, Integer>> firstLetterToLength =
        pipeline
            .apply("ReadLines", TextIO.read().from(options.getInputFile()))
            .apply("ExtractFirstLetterAndLength", ParDo.of(new ExtractFirstLetterAndLengthFn()));

    firstLetterToLength
        // Combine.perKey groups by key (first letter) and reduces each group's values with our
        // custom CombineFn, in a distributed, partial-aggregation-then-merge fashion.
        .apply("AverageLengthPerFirstLetter", Combine.perKey(new AverageFn()))
        .apply(
            "FormatResults",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + ": " + String.format("%.2f", kv.getValue())))
        .apply("WriteResults", TextIO.write().to(options.getOutput()).withSuffix(".txt"));

    pipeline.run().waitUntilFinish();
  }
}
