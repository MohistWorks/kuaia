# Changelog

All notable public changes to Kuaia are tracked here.

## Unreleased

## 0.1.2 - 2026-05-14

### Added

- File sources now support `source.format: jsonl` for local JSON Lines
  pipelines, including a public JSONL-to-vector example and smoke coverage.
- Added a `chunk` transform for character-based text chunking before embedding,
  including a public JSONL chunk-to-vector example and smoke coverage.
- Qdrant sinks can now generate stable chunk point ids with
  `chunkIndexField` and `chunkIdMultiplier`, with a public JSONL
  chunk-to-Qdrant example.
- Chunk transforms now support `dropInput` and `includeOffsets` so chunked
  vector payloads can omit repeated source text while keeping character offsets.
- Added a minimal `filter` transform with `op: not-empty` for dropping empty
  text rows before embedding or chunking.
- Added a `trim` transform for cleaning leading and trailing string whitespace
  before filtering, chunking, or embedding.
- Added a `lowercase` transform for deterministic string normalization before
  filtering, chunking, or embedding.
- Added a `replace` transform for literal string replacement in local text
  cleanup pipelines.
- `filter` now supports `op: starts-with` and `op: ends-with` for
  case-sensitive literal prefix and suffix filtering.
- `filter` now supports `op: equals` and `op: not-equals` for case-sensitive
  exact-match filtering.
- `filter` now supports `op: greater-than`, `op: greater-than-or-equal`,
  `op: less-than`, and `op: less-than-or-equal` for `LONG` fields.
- File sinks now support `format: jsonl`, including a public JSONL clean-to-file
  example and smoke coverage.
- `filter` now supports `op: min-length` for dropping too-short text before
  file output, chunking, or embedding.

### Changed

- CSV file sinks now quote string values containing commas, quotes, or line
  breaks instead of rejecting them.
- CSV file sources now parse quoted fields with commas, escaped quotes, and
  line breaks as one logical source record.
- Added a public quoted CSV source-to-file example and smoke coverage.
- `filter` now supports `op: contains` for case-sensitive substring filtering
  in local text cleanup pipelines.

## 0.1.1 - 2026-05-13

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
- Replayed worker assignments now include serialized task definitions so
  reconnecting workers receive executable assignment payloads.
- Worker stream messages with conflicting envelope and payload worker ids are
  now ignored to keep stream identity, registry, and persisted worker state
  aligned.
- Worker nodes now send a typed `WorkerHello` immediately after opening the
  task stream, enabling coordinator recovery replay and stream state tracking.
- Worker nodes now respond to typed `TaskAssignment` messages with typed
  attempt results, closing the coordinator replay path beyond legacy task
  payloads.
- Worker nodes now reject malformed typed assignments missing task or attempt
  ids with `INVALID_ASSIGNMENT` instead of reporting success.
- Worker nodes now also require typed assignments to include definition bytes
  before reporting success.
- Worker assignment definition bytes must now deserialize to `TaskDefinition`
  before the worker reports a typed assignment as successful.
- Worker assignment definitions must now carry the same task id as the
  assignment envelope before the worker reports success.
- Worker nodes now reject expired typed assignments instead of reporting
  success after the assignment lease has elapsed.
- Worker hello replay now moves expired active assignments back to retrying
  instead of resending stale leases to reconnecting workers.
- Coordinator ack handling now rejects expired attempts before applying typed
  record, checkpoint, or task-result acknowledgements.
- Transient typed task-attempt failures now move tasks to RETRYING while
  preserving checkpoint progress instead of marking them permanently failed.
- RETRYING tasks now clear stale worker assignments and are no longer replayed
  as active worker assignments after coordinator recovery.
- `kuaia benchmark` can now write CSV benchmark output with `--format csv`.
- `kuaia benchmark` can now run caller-selected batch sizes with
  `--batch-sizes`.
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
