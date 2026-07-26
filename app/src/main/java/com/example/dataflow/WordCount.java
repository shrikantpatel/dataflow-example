package com.example.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * A minimal, first Apache Beam pipeline: classic WordCount.
 *
 * <p>Core Beam concepts demonstrated here:
 * <ul>
 *   <li>{@link Pipeline} - the object that represents the whole data processing job/graph.
 *   <li>{@link PCollection} - an immutable, distributed collection of data flowing through the
 *       pipeline (can represent a bounded batch or unbounded stream).
 *   <li>{@link org.apache.beam.sdk.transforms.PTransform} - an operation that takes one or more
 *       PCollections and produces a PCollection (e.g. ParDo, Count, MapElements).
 *   <li>{@link PipelineOptions} - command-line configurable options (input/output paths, runner,
 *       etc.).
 * </ul>
 *
 * <p>Run it locally (uses the DirectRunner, which executes on your machine, no cloud needed):
 *
 * <pre>{@code
 * ./gradlew run --args="--inputFile=src/main/resources/input.txt --output=build/output/counts"
 * }</pre>
 */
public class WordCount {

  /** Splits each line of text into individual words. */
  static class ExtractWordsFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<String> receiver) {
      String[] words = line.split("[^\\p{L}]+");
      for (String word : words) {
        if (!word.isEmpty()) {
          receiver.output(word.toLowerCase());
        }
      }
    }
  }

  /** Formats a (word, count) pair into a printable string, e.g. "beam: 3". */
  static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  /** Custom pipeline options: adds --inputFile and --output flags on top of the Beam defaults. */
  public interface WordCountOptions extends PipelineOptions {
    @Description("Path to the input text file to read")
    @Default.String("src/main/resources/input.txt")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path prefix for the output file(s)")
    @Default.String("build/output/counts")
    String getOutput();

    void setOutput(String value);
  }

  public static void main(String[] args) {
    WordCountOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        // 1. Read lines of text from the input file into a PCollection<String>.
        .apply("ReadLines", TextIO.read().from(options.getInputFile()))
        // 2. Split each line into individual words.
        .apply("ExtractWords", ParDo.of(new ExtractWordsFn()))
        // 3. Count occurrences of each unique word -> PCollection<KV<String, Long>>.
        .apply("CountWords", Count.perElement())
        // 4. Format each (word, count) pair as a human-readable string.
        .apply("FormatResults", MapElements.via(new FormatAsTextFn()))
        // 5. Write the results out to a file.
        .apply("WriteResults", TextIO.write().to(options.getOutput()).withSuffix(".txt"));

    pipeline.run().waitUntilFinish();
  }
}
