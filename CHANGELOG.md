# Changelog

All notable public changes to Kuaia are tracked here.

## Unreleased

### Changed

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
