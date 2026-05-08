# Kuaia

Kuaia is an experimental Java runtime for AI-ready data integration. The current repository is an MVP focused on execution semantics: local pipelines, typed task attempts, checkpoint-aware state, RocksDB persistence, and a bounded Raft HA prototype.

## Current Status

Implemented today:

- `BinaryRow` with primitive, string, and vector fields.
- Connector interfaces for source, transform, and sink components.
- Local demo pipelines for structured rows and mock AI vector output.
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

## Build And Test

```bash
mvn -q test
```

Install the local module artifacts before running the CLI through Maven:

```bash
mvn -q install
```

## Run The Demos

Use the engine CLI through Maven's exec plugin:

```bash
mvn -q -pl kuaia-engine exec:java \
  -Dexec.mainClass=com.kuaia.engine.KuaiaCli \
  -Dexec.args=help
```

Run the local row pipeline:

```bash
mvn -q -pl kuaia-engine exec:java \
  -Dexec.mainClass=com.kuaia.engine.KuaiaCli \
  -Dexec.args=local-demo
```

Run the mock AI vector pipeline:

```bash
mvn -q -pl kuaia-engine exec:java \
  -Dexec.mainClass=com.kuaia.engine.KuaiaCli \
  -Dexec.args=ai-demo
```

Run the recovery demo with a RocksDB state directory:

```bash
mvn -q -pl kuaia-engine exec:java \
  -Dexec.mainClass=com.kuaia.engine.KuaiaCli \
  -Dexec.args="recover-demo --state-dir /tmp/kuaia-recover-demo"
```

## Repository Layout

- `kuaia-common`: shared data model, connector APIs, protobuf contracts, and common utilities.
- `kuaia-engine`: local execution, worker/coordinator runtime components, state stores, Raft integration, and runnable demos.
- `docs/visuals`: public visual references.

Internal design notes are intentionally not part of the public documentation tree. Public user-facing docs should live in `README.md` or future files under `docs/`.

## Development Notes

The codebase is intentionally small and test-first. Before making behavioral changes, add a focused failing test, implement the minimum production code, and run:

```bash
mvn -q test
git diff --check
```
