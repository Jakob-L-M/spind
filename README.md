# SPIND

SPIND discovers unary and n-ary partial inclusion dependencies. It can be run through Metanome or through the standalone `runner.Runner` entry point.

## Build

SPIND requires **Java 17 or newer** and Maven. The Metanome adapter targets `de.metanome:algorithm_integration:1.2-SNAPSHOT`, including the `RelaxedInclusionDependency` API available in the sibling `../Metanome` checkout.

Set `JAVA_HOME` to a supported build JDK (for example, JDK 17, 21, or 22). Lombok currently does not compile this project on JDK 27; the build and tests have been verified with JDK 22. The resulting JAR targets Java 17.

From the SPIND directory, install that API locally, then build SPIND:

```sh
mvn -f ../Metanome/pom.xml -pl algorithm_integration -am install \
  -Dmaven.test.skip=true -Dmaven.javadoc.skip=true
mvn clean verify
```

The build produces:

- `target/spind-1.0.0-SNAPSHOT.jar`: the regular application JAR.
- `target/spind-1.0.0-SNAPSHOT-metanome.jar`: the Metanome plugin, including
  SPIND's runtime dependencies. Use this JAR for Metanome execution.

The Metanome API is provided by the host. SPIND's bundled Guava, OpenCSV, Commons,
and logging libraries are relocated to avoid conflicts with Metanome's versions.
The plugin manifest declares `metanome.SpindAlgorithm` as its bootstrap class.

## Run through Metanome's UI

1. Run Metanome itself on Java 17 or newer.
2. Copy `target/spind-1.0.0-SNAPSHOT-metanome.jar` into the running deployment's algorithm directory (typically `WEB-INF/classes/algorithms`), then refresh Metanome's algorithm list.
3. Select SPIND under **Relaxed Inclusion Dependency** algorithms.
4. Select one or more relational inputs under `INPUT_FILES`, configure the parameters below, and execute.

Results are delivered to Metanome with the original relation and column names, ordered dependent/referenced column combinations, and SPIND's partial degree as the relaxed-dependency measure. This uses the same partial-degree calculation as SPIND's standalone JSON output. CSV parsing settings, including the null marker, are configured on the Metanome input.

## Algorithm parameters

All algorithm parameters have defaults. `INPUT_FILES` requires at least one input; relation names must be distinct and each input must have a column schema.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `THRESHOLD` | `0.93` | Minimum inclusion degree in `[0, 1]`; supplied as a string because Metanome has no double parameter type. |
| `MAX_ARITY` | `-1` | Maximum number of columns per dependency; `1` for unary discovery, `-1` for unlimited. |
| `PARALLELISM` | Available processors | Number of chunking, sorting, and merging workers. |
| `CHUNK_SIZE` | `6000000` | Target number of input values per chunk. |
| `SORT_SIZE` | `10000000` | Sorting buffer size, shared across workers; must be at least `PARALLELISM`. |
| `MERGE_SIZE` | `500` | Merge fan-in budget; must be at least `2`. |
| `VALIDATION_SIZE` | `50000` | Validation buffer budget, shared across active relations. |
| `NULL_HANDLING` | `SUBSET` | One of `SUBSET`, `FOREIGN`, `EQUALITY`, or `INEQUALITY`, using SPIND's null semantics. |
| `DUPLICATE_HANDLING` | `AWARE` | `AWARE` counts row occurrences; `UNAWARE` counts distinct values/tuples. |
| `USE_FILTER` | `true` | Use SPIND's Bloom filter to mask irrelevant values in subsequent layers. |
| `REFINE_FILTER` | `true` | Rebuild the Bloom filter in each layer. |

Buffer sizes and parallelism must be positive. Metanome inputs preserve the distinction between actual nulls and empty strings, as well as quoted, multiline, and Unicode values, using a lossless internal chunk encoding.

## Standalone execution

Configure the dataset and filesystem paths in `runner.Runner` / `runner.Config`, create the configured temporary and result directories, and invoke `runner.Runner`. The existing standalone CSV input and JSON output are supported.
