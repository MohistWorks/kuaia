# Connector Development

Kuaia's connector surface is still an MVP boundary. It is useful for
contributors who want to understand or extend the local runtime, but it is not
yet a stable plugin SDK or production-certified connector framework.

## Current Shape

The local runner uses this public pipeline shape:

```text
Source -> BinaryRow -> Transforms -> Sink
```

Rows are represented by `BinaryRow` and described by `KuaiaRowType`. Current
field types include primitive values, strings, and vectors.

## Source Boundary

Current built-in sources are:

- `FileSource` for local CSV and JSONL files,
- `DuckDBSource` for one bounded local DuckDB SQL query,
- `PostgresSource` for one bounded JDBC query,
- `MySQLSource` for one bounded JDBC query.

Local sources implement `LocalSource`:

```java
void open() throws Exception;
int readFrom(long lastCheckpointSeq, RecordConsumer consumer, RecordErrorConsumer errorConsumer) throws Exception;
KuaiaRowType getRowType();
void close() throws Exception;
```

The v2 adapter boundary is split-aware:

```java
List<SourceSplit> enumerateSplits() throws Exception;
BatchSourceReader createReader(SourceSplit split) throws Exception;
```

For the current MVP, file sources can produce bounded `SourceSplit` ranges.
Other current sources use a single split. CDC offsets, streaming reads, and
source-side exactly-once coordination are not part of the public contract yet.

## Transform Boundary

Transforms implement `PipelineTransform`:

```java
KuaiaRowType outputType(KuaiaRowType inputType) throws PipelineExecutionException;
BinaryRow apply(BinaryRow input) throws PipelineExecutionException;
List<BinaryRow> applyBatch(List<BinaryRow> inputs) throws PipelineExecutionException;
int preferredBatchSize();
```

Use `applyBatch` and `preferredBatchSize` when a transform benefits from
batching, such as embedding requests. Row-by-row transforms can implement
`apply` and use the default batch behavior.

## Sink Boundary

Current built-in sinks are:

- `console`,
- local CSV `file`,
- `mock-vector`,
- `qdrant`.

Sinks implement `SinkWriter`:

```java
void open() throws Exception;
void write(BinaryRow row) throws Exception;
void writeBatch(List<BinaryRow> rows) throws Exception;
void close() throws Exception;
```

The local runner adapts `SinkWriter` into the batch-aware v2 sink boundary:

```java
void writeBatch(List<BinaryRow> rows) throws Exception;
SinkCommitter committer();
```

Vector sinks are currently registered through `SinkFactoryRegistry`. Adding a
new YAML `sink.type` still requires code changes in the registry and the YAML
loader. There is no dynamic connector discovery or external plugin loading yet.

## Adding A Connector Today

For a small source:

1. Add a focused source class under `kuaia-engine/src/main/java/.../connector`.
2. Add unit tests for open, type inference, row reading, checkpoint skipping,
   and failure messages.
3. Wire the source into `PipelineConfigLoader` and `LocalPipelineRunner`.
4. Add a YAML example only if it can run locally without hidden services.
5. Update `docs/pipeline-yaml.md` with the exact YAML fields and limitations.

For a small sink:

1. Add a focused sink class or vector sink factory.
2. Add unit tests for required fields, supported row types, batch writes, and
   I/O failure wrapping.
3. Register the sink in `SinkFactoryRegistry` or `LocalPipelineRunner`.
4. Add docs and an example only when the example can be run by a contributor.

For a transform:

1. Add a `PipelineTransform` implementation.
2. Add schema tests for `outputType`.
3. Add row and batch tests for `apply` or `applyBatch`. Transforms that expand
   one input row into multiple output rows should implement `applyBatch`.
4. Wire the transform into `PipelineConfigLoader` and `TransformPipeline`.
5. Document the YAML fields and unsupported cases.

## Validation

Run focused tests for the touched connector first, then the public MVP smoke:

```bash
mvn -q -pl kuaia-engine -am -Dtest=KuaiaExamplesTest test
make public-mvp-smoke
```

Before publishing connector changes, run the full local gate:

```bash
mvn -q test
mvn -q package
git diff --check
```
