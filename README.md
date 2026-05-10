# Kuaia

Kuaia is an experimental Java runtime for AI-ready data integration. The
long-term product direction is an AI-ready DataOps engine for moving,
transforming, vectorizing, and activating data with predictable infrastructure
costs.

The current open-source repository is intentionally narrower. It is an MVP for
developers who want to run local, declarative, checkpoint-aware batch pipelines
and evaluate the core execution semantics before Kuaia grows into a broader
platform.

For the user-facing product boundary, see [`docs/product-scope.md`](docs/product-scope.md).

## Product Scope

Kuaia today is best understood as a local AI data pipeline runtime:

- It reads bounded data from supported sources.
- It carries rows through a typed `BinaryRow` model.
- It applies a linear transform chain, including embedding transforms.
- It writes to local outputs or vector sinks.
- It persists checkpoint progress so reruns can resume completed work.

Kuaia is not yet a general ETL platform, a CDC engine, a distributed DAG
runtime, or a visual DataOps product. Those are roadmap directions, not current
MVP promises.

## Current Status

Implemented today:

- `BinaryRow` with primitive, string, and vector fields.
- Connector interfaces for source, transform, and sink components.
- Local demo pipelines for structured rows and mock AI vector output.
- Declarative local YAML pipelines with checkpointed `select`, `rename`, and `mock-embedding` transforms.
- Declarative `file` and batch `postgres` sources with `console`, `file`, `mock-vector`, and `qdrant` sinks.
- Adapter-first connector API v2 boundary with split-aware source readers and batch sink writers.
- Local mock embedding provider, OpenAI-compatible embedding provider, and mock vector sink registries for future extension.
- Coordinator/Worker protocol models for task assignment, ack, checkpoint, backpressure, and attempt results.
- In-memory and RocksDB-backed task/worker state stores.
- Coordinator recovery planning for expired task leases.
- Ratis/Raft state-store integration tests for the MVP HA path.

Not implemented yet:

- Production-grade installers or release artifacts.
- CDC, streaming, or production-certified external connectors.
- Additional vector database integrations beyond Qdrant.
- Web UI, RBAC, audit, lineage, or Kubernetes operator support.
- Exactly-once guarantees. The MVP target is at-least-once with idempotent sinks.

## Requirements

- JDK 8 or newer
- Maven 3.8+
- Docker with Compose support, optional

## Quick Start

```bash
bin/kuaia help
bin/kuaia examples
bin/kuaia run -f examples/local-file-to-file.yaml
cat .kuaia/output/local-file-to-file.csv
```

To validate the public MVP paths without external services:

```bash
make public-mvp-smoke
```

The supported pipeline YAML contract is documented in
[`docs/pipeline-yaml.md`](docs/pipeline-yaml.md). The public examples are
listed in [`docs/examples.md`](docs/examples.md). Connector extension notes are
in [`docs/connector-development.md`](docs/connector-development.md). Open-source
release candidate checks are listed in
[`docs/release-checklist.md`](docs/release-checklist.md).

To build a packaged runtime:

```bash
mvn -q package
java -jar kuaia-engine/target/kuaia-engine-0.1.0-SNAPSHOT-cli.jar help
```

After `mvn -q package`, `bin/kuaia` uses the packaged jar automatically. In a
fresh checkout before packaging, the script falls back to Maven so examples can
still be run directly.

Or use Make aliases:

```bash
make test
make public-mvp-smoke
make run-vector
make clean-state
```

Or run the deterministic file-output example through Docker Compose:

```bash
docker compose up --build
```

The Docker quickstart runs `examples/local-file-to-file.yaml`. Its output is
written inside the container at `.kuaia/output/local-file-to-file.csv`, backed by
the `kuaia-state` Compose volume. For local host output, run the same example
with `bin/kuaia`.

## Build And Test

```bash
mvn -q test
mvn -q package
```

## Run The Demos

Use the short wrapper:

```bash
bin/kuaia help
bin/kuaia examples
```

Run the local row pipeline:

```bash
bin/kuaia local-demo
```

Run a declarative local pipeline from YAML:

```bash
bin/kuaia run -f examples/local-file-to-console.yaml
```

When the YAML includes `checkpoint.stateDir`, Kuaia persists local progress after
successful sink writes. Batch-aware vector pipelines advance the checkpoint once
per successful sink batch to the highest source sequence id in that batch.
Re-running the same pipeline resumes after the last committed row instead of
re-emitting completed rows.

Run a declarative pipeline with a simple transform chain:

```bash
bin/kuaia run -f examples/local-file-transform-to-console.yaml
```

Run a declarative pipeline that writes CSV output:

```bash
bin/kuaia run -f examples/local-file-to-file.yaml
cat .kuaia/output/local-file-to-file.csv
```

Run a declarative pipeline that skips malformed CSV records:

```bash
bin/kuaia run -f examples/local-file-skip-bad-records.yaml
```

Run a declarative AI-ready vector pipeline:

```bash
bin/kuaia run -f examples/local-file-to-vector.yaml
```

Run a declarative vector pipeline against an OpenAI-compatible embeddings API:

```bash
export OPENAI_API_KEY=...
bin/kuaia run -f examples/local-file-to-openai-compatible-vector.yaml
```

The OpenAI-compatible embedding transform supports `batchSize` for batched array
input requests and `timeoutMs` for HTTP connect/read timeout control. For
stricter local file runs, set `KUAIA_RESTRICT_LOCAL_PATHS=true` to reject YAML
paths outside the YAML directory or repository `.kuaia/`.

Run a declarative vector pipeline into Qdrant:

```bash
docker compose -f docker-compose.qdrant.yml up -d
curl -X PUT http://localhost:6333/collections/kuaia_docs \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
bin/kuaia run -f examples/local-file-to-qdrant.yaml
```

Run a batch Postgres-to-Qdrant pipeline:

```bash
docker compose -f docker-compose.postgres.yml -f docker-compose.qdrant.yml up -d
curl -X PUT http://localhost:6333/collections/kuaia_pg_docs \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
export KUAIA_POSTGRES_USER=kuaia
export KUAIA_POSTGRES_PASSWORD=kuaia
bin/kuaia run -f examples/postgres-to-qdrant.yaml
```

Run the mock AI vector pipeline:

```bash
bin/kuaia ai-demo
```

Run the recovery demo with a RocksDB state directory:

```bash
bin/kuaia recover-demo --state-dir /tmp/kuaia-recover-demo
```

Before packaging, the wrapper falls back to Maven. The lower-level command is
still available when you need to bypass the script:

```bash
mvn -q -pl kuaia-engine exec:java \
  -Dexec.mainClass=com.kuaia.engine.KuaiaCli \
  -Dexec.args="run -f examples/local-file-to-vector.yaml"
```

Declarative runs print a stable summary line with rows read, rows written, rows
failed under the configured bad-record policy, rows skipped by checkpoint
resume, latest checkpoint sequence, task state, source splits, sink batches, and
duration in milliseconds.

## Benchmarks

Kuaia includes a small local benchmark smoke test for the batch-aware pipeline
path. It uses generated CSV data, a counting embedding provider, a counting
vector sink, and local checkpoint state; it does not call OpenAI, Qdrant, or any
external service. The JSON output includes row, embedding, checkpoint, source
split, and sink batch counters.

```bash
mvn -q -pl kuaia-engine -am -Dtest=LocalPipelineBenchmarkTest -Dsurefire.failIfNoSpecifiedTests=false test
cat kuaia-engine/target/kuaia-benchmark/local-pipeline-batch.json
```

The default smoke uses 128 rows so CI stays fast. For a larger local run:

```bash
mvn -q -pl kuaia-engine -am -Dtest=LocalPipelineBenchmarkTest -Dkuaia.benchmark.rows=10000 -Dsurefire.failIfNoSpecifiedTests=false test
```

To stress source split behavior, add `-Dkuaia.benchmark.maxRowsPerSplit=<rows>`.

## Repository Layout

- `kuaia-common`: shared data model, connector APIs, protobuf contracts, and
  common utilities.
- `kuaia-engine`: local execution, worker/coordinator runtime components, state
  stores, Raft integration, and runnable demos.
- `scripts`: local developer and public MVP smoke checks.
- `docs/visuals`: public visual references.

Internal design notes are intentionally not part of the public documentation
tree. Public user-facing docs should live in `README.md` or future files under
`docs/`.

## License

Kuaia is licensed under the Apache License, Version 2.0. See `LICENSE`.

## Security

Please report suspected vulnerabilities through GitHub Security Advisories. See
[`SECURITY.md`](SECURITY.md) for the current MVP security policy.

## Development Notes

The codebase is intentionally small and test-first. Before making behavioral changes, add a focused failing test, implement the minimum production code, and run:

```bash
mvn -q test
git diff --check
```
