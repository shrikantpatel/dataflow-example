# dataflow-example

A first hands-on project for learning **Apache Beam** (the open-source SDK that also powers Google Cloud Dataflow) in Java. This repo will grow over time as we explore more Beam/Dataflow concepts.

## What's here

A classic **WordCount** pipeline — the "hello world" of Apache Beam — that:
1. Reads a text file (`PCollection<String>` of lines)
2. Splits each line into words (`ParDo`)
3. Counts occurrences of each unique word (`Count.perElement()`)
4. Formats and writes results to an output file (`MapElements`, `TextIO.write`)

It runs on the **DirectRunner**, which executes entirely on your local machine — no cloud account or Dataflow service needed for this stage of learning.

## Stack

- **Java 25** installed on the machine, but the Gradle build uses a **Java 17 toolchain** to compile/run — Apache Beam 2.75.0 does not yet officially support Java 25, and 17 is the latest LTS it fully supports. Gradle auto-selects your local Homebrew JDK 17 install; no download needed.
- **Gradle** (application plugin)
- **Apache Beam SDK** 2.75.0 (`beam-sdks-java-core`, `beam-runners-direct-java`)
- **JUnit 4 + Hamcrest** for pipeline tests, using Beam's `TestPipeline` and `PAssert`

## Project layout

```
dataflow-example/
├── settings.gradle
└── app/
    ├── build.gradle
    └── src/
        ├── main/java/com/example/dataflow/WordCount.java
        ├── main/resources/input.txt        # sample input text
        └── test/java/com/example/dataflow/WordCountTest.java
```

## Running it

Build and run tests:
```bash
gradle build
```

Run the pipeline with default input/output paths:
```bash
gradle :app:run
```

Run with custom input/output:
```bash
gradle :app:run --args="--inputFile=src/main/resources/input.txt --output=build/output/counts"
```

Output is written as sharded text files, e.g. `app/build/output/counts-00000-of-00004.txt`, each line like `beam: 4`.

## Key Beam concepts introduced

| Concept | What it is |
|---|---|
| `Pipeline` | The whole job graph you build and then run |
| `PCollection<T>` | An immutable, distributed collection flowing through the pipeline |
| `PTransform` | An operation on PCollection(s) — e.g. `ParDo`, `Count`, `MapElements`, `TextIO` |
| `DoFn` | User code applied per-element inside a `ParDo` |
| `PipelineOptions` | Command-line configurable settings (runner, input/output paths, etc.) |
| `Runner` | The engine executing the pipeline — here, `DirectRunner` (local) |

## Ideas for future iterations

- Windowing & triggers with an unbounded/streaming source
- Side inputs and side outputs
- Combine transforms (`Combine.perKey`, custom `CombineFn`)
- Reading/writing structured data (Avro, JSON, BigQuery-style schemas)
- Running the same pipeline on other runners (e.g. Flink) or actual Google Cloud Dataflow
- Beam SQL / Schema-aware PCollections
