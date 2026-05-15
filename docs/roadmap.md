# Roadmap

Kuaia is currently a local, checkpoint-aware AI-ready data pipeline runtime. The
`0.1.3` line should improve that local RAG data preparation experience before
the project expands into distributed execution, CDC, or a larger connector
catalog.

## 0.1.3 Theme

Make Kuaia a more reliable and easier-to-integrate local RAG data preparation
tool.

The target user should be able to take bounded CSV, JSONL, or PostgreSQL data,
clean and filter it, preserve useful metadata, chunk text, generate embeddings,
write to a file or Qdrant, and quickly diagnose failures from the CLI output.

## Planned 0.1.3 Focus Areas

### 1. Pipeline Diagnostics

- Add a structured run summary output path, such as optional JSON summary
  output.
- Keep CLI failure messages stable and tied to source, transform, or sink
  stages.
- Cover diagnostic output in public examples and smoke tests.

### 2. RAG Metadata Handling

- Clarify how document identifiers and metadata fields flow through JSONL and
  PostgreSQL pipelines.
- Make Qdrant payload fields more explicit and predictable.
- Preserve stable chunk metadata such as document id, chunk index, offsets, and
  selected source metadata.

### 3. Connector Experience

- Strengthen batch connector documentation around source readers, sink writers,
  split handling, and batch writes.
- Keep PostgreSQL as the production-like batch source reference.
- Evaluate a MySQL batch source only after the connector boundary is clear
  enough for contributors to follow.

### 4. Embedding And Vector Reliability

- Tighten OpenAI-compatible embedding error handling around response shape,
  timeout behavior, and batch failures.
- Keep Qdrant upsert ids, payloads, and batch behavior deterministic.
- Prefer mock-backed coverage for behavior that should not require external
  services in CI.

### 5. Developer Workflow

- Consider a `kuaia validate -f <pipeline.yaml>` style preflight command for
  YAML and connector configuration checks.
- Improve example discovery around common RAG flows: local document import, FAQ
  import, and PostgreSQL content to Qdrant.
- Keep release gates simple: Maven tests, package, CLI help, public smoke, and
  Docker Compose config.

## Not In 0.1.3

The following remain future work and should not block `0.1.3`:

- CDC or streaming sources,
- transform DAGs, joins, branches, or fan-out,
- exactly-once execution,
- Kubernetes or distributed HA operation,
- web UI, RBAC, lineage, or governance features,
- large connector catalog expansion,
- broad vector database support beyond Qdrant.

## Release Criteria

`0.1.3` is ready when the documented focus areas have either shipped or been
explicitly deferred, and the public validation gate passes:

```bash
mvn -q test
mvn -q package
bin/kuaia help
bin/kuaia examples
bin/kuaia validate -f examples/local-file-to-file.yaml
bin/kuaia validate -f examples/local-jsonl-chunk-to-qdrant.yaml
bin/kuaia validate -f examples/postgres-to-qdrant.yaml
make public-mvp-smoke
docker compose config
git diff --check
```
