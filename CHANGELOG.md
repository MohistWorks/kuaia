# Changelog

All notable public changes to Kuaia are tracked here.

## Unreleased

### Added

- `kuaia benchmark` CLI command for local batch benchmark counters and JSON
  output.

### Changed

- Ratis-backed StateStore now supports worker records and task/worker scan
  queries used by coordinator recovery paths.
- Deprecated Ratis task state reads now prefer v2 task records and state scans
  instead of stale legacy keys.
- Ratis-backed task compare-and-set now reports stale-version conflicts as a
  false update result instead of surfacing them as Raft write failures.
- Coordinator worker streams now persist WorkerRecord online, paused, and
  offline states in StateStore for recovery-aware scheduling.
- Worker heartbeat RPCs now update persisted WorkerRecord load score and
  heartbeat timestamp.
- Scheduler can use persisted WorkerRecord state to filter offline/paused
  workers and order available workers by stored load.
- Legacy ack flushing now completes v2 TaskRecord state through compare-and-set
  before falling back to deprecated state writes.
- Deprecated task-definition saves now preserve TaskDefinition in v2 TaskRecord
  scans, including the Ratis CREATED-task path.
- Deprecated task-definition saves now honor the requested TaskState through the
  v2 TaskRecord path.
- Deprecated task-state updates now update existing v2 TaskRecord state before
  falling back to legacy Ratis state keys.
- Legacy ack flushing now drops unknown-task acknowledgements when the backing
  StateStore has no legacy state path to update.
- Worker streams now preserve the last valid worker identity when ignoring
  malformed empty worker messages, so disconnect handling still marks the
  original worker offline.
- Worker hello messages now clear stale stream-level backpressure so reconnects
  can become schedulable again without letting ack-only messages reset pause
  state.
- Worker streams now ignore messages that try to switch an established stream
  to a different worker id, preserving disconnect/offline bookkeeping for the
  original worker.
- RETRYING tasks now clear stale worker assignments and are no longer replayed
  as active worker assignments after coordinator recovery.
- `kuaia benchmark` can now write CSV benchmark output with `--format csv`.
- `kuaia benchmark` can now run caller-selected batch sizes with
  `--batch-sizes`.
- Main branch development version is now `0.1.1-SNAPSHOT` after the `v0.1.0`
  release.
- Runtime packaging examples resolve the shaded CLI jar without hard-coding the
  release version.

## 0.1.0 - 2026-05-11

### Added

- Local, declarative, checkpoint-aware batch pipeline runtime.
- Typed `BinaryRow` data model with primitive, string, and vector fields.
- YAML pipeline runner with `select`, `rename`, `mock-embedding`, and
  OpenAI-compatible embedding transforms.
- Batch-aware embedding and sink paths for vector pipelines.
- File and batch Postgres sources.
- Console, file, mock-vector, and Qdrant sinks.
- Adapter-first connector API v2 boundary with split-aware source readers and
  batch sink writers.
- Coordinator/Worker protocol models for task assignment, ack, checkpoint,
  backpressure, and attempt results.
- In-memory, RocksDB-backed, and Ratis-backed state-store coverage for the MVP
  coordination path.
- Public MVP smoke script, CLI example listing, Docker Compose quickstart, and
  GitHub Actions CI coverage.
- Public product scope, YAML, example, connector-development, security, release
  checklist, and documentation index files.

### Current MVP Boundary

- Kuaia is currently a local AI-ready data pipeline runtime.
- The MVP targets at-least-once progress with idempotent sinks, not exactly-once
  processing.
- Internal design notes are kept outside the public documentation tree.

### Not Included Yet

- Binary installers or package-manager distribution.
- CDC, streaming, or production-certified external connectors.
- Distributed DAG execution, shuffle, joins, fan-out, or Kubernetes operation.
- Additional vector database integrations beyond Qdrant.
- Web UI, RBAC, audit, or lineage features.

### Recommended Validation

```bash
mvn -q test
mvn -q package
make public-mvp-smoke
docker compose config
git diff --check
```
