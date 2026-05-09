# Kuaia

Kuaia is an experimental Java runtime for AI-ready data integration. The current repository is an MVP focused on execution semantics: local pipelines, typed task attempts, checkpoint-aware state, RocksDB persistence, and a bounded Raft HA prototype.

## Current Status

Implemented today:

- `BinaryRow` with primitive, string, and vector fields.
- Connector interfaces for source, transform, and sink components.
- Local demo pipelines for structured rows and mock AI vector output.
- Declarative local YAML pipelines with checkpointed `select`, `rename`, and `mock-embedding` transforms.
- Declarative `console`, `file`, and `mock-vector` local sinks with CLI run summaries.
- Coordinator/Worker protocol models for task assignment, ack, checkpoint, backpressure, and attempt results.
- In-memory and RocksDB-backed task/worker state stores.
- Coordinator recovery planning for expired task leases.
- Ratis/Raft state-store integration tests for the MVP HA path.

Not implemented yet:

- Production deployment packaging.
- Real external connectors.
- Real embedding providers or vector databases.
- Web UI, RBAC, audit, lineage, or Kubernetes operator support.
- Exactly-once guarantees. The MVP target is at-least-once with idempotent sinks.

## Requirements

- JDK 8 or newer
- Maven 3.8+
- Docker with Compose support, optional

## Quick Start

```bash
bin/kuaia help
bin/kuaia run -f examples/local-file-to-vector.yaml
```

The supported pipeline YAML contract is documented in
[`docs/pipeline-yaml.md`](docs/pipeline-yaml.md).

Or use Make aliases:

```bash
make test
make run-vector
make clean-state
```

Or run the default vector example through Docker Compose:

```bash
docker compose up --build
```

## Build And Test

```bash
mvn -q test
```

## Run The Demos

Use the short wrapper:

```bash
bin/kuaia help
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
each successfully written CSV row. Re-running the same pipeline resumes after the
last committed row instead of re-emitting completed rows.

Run a declarative pipeline with a simple transform chain:

```bash
bin/kuaia run -f examples/local-file-transform-to-console.yaml
```

Run a declarative pipeline that writes CSV output:

```bash
bin/kuaia run -f examples/local-file-to-file.yaml
cat .kuaia/output/local-file-to-file.csv
```

Run a declarative AI-ready vector pipeline:

```bash
bin/kuaia run -f examples/local-file-to-vector.yaml
```

Run the mock AI vector pipeline:

```bash
bin/kuaia ai-demo
```

Run the recovery demo with a RocksDB state directory:

```bash
bin/kuaia recover-demo --state-dir /tmp/kuaia-recover-demo
```

The wrapper delegates to Maven. The lower-level command is still available when
you need to bypass the script:

```bash
mvn -q -pl kuaia-engine exec:java \
  -Dexec.mainClass=com.kuaia.engine.KuaiaCli \
  -Dexec.args="run -f examples/local-file-to-vector.yaml"
```

Declarative runs print a stable summary line with rows read, rows written, rows
skipped by checkpoint resume, latest checkpoint sequence, task state, and
duration in milliseconds.

## Repository Layout

- `kuaia-common`: shared data model, connector APIs, protobuf contracts, and common utilities.
- `kuaia-engine`: local execution, worker/coordinator runtime components, state stores, Raft integration, and runnable demos.
- `docs/visuals`: public visual references.

Internal design notes are intentionally not part of the public documentation tree. Public user-facing docs should live in `README.md` or future files under `docs/`.

## License

Kuaia is licensed under the Apache License, Version 2.0. See `LICENSE`.

## Development Notes

The codebase is intentionally small and test-first. Before making behavioral changes, add a focused failing test, implement the minimum production code, and run:

```bash
mvn -q test
git diff --check
```
