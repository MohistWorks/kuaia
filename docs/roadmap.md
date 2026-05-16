# Roadmap

Kuaia is currently a local, checkpoint-aware AI-ready data pipeline runtime.
The `0.1.x` line proved the public MVP: local YAML pipelines can read bounded
data, clean and vectorize rows, write to files or Qdrant, resume from local
checkpoints, and expose deterministic validation and run summaries.

## 0.1.3 shipped

`0.1.3` closed the public MVP release line on 2026-05-15.

Shipped in the `0.1.3` line:

- JSON run summaries through `kuaia run --summary-json <path>`.
- Stable source, transform, sink, and checkpoint failure prefixes.
- `kuaia validate -f <pipeline.yaml>` preflight checks for YAML and local file
  pipelines.
- Explicit Qdrant `payloadFields` for predictable vector metadata.
- Stable chunk metadata for JSONL document chunking, including chunk index and
  character offsets.
- No-service example discovery for common RAG flows: local document import, FAQ
  import, and PostgreSQL content to Qdrant.
- OpenAI-compatible embedding response validation for batch count, duplicate
  index, and out-of-range index failures.
- Qdrant upsert response validation for application-level `status` failures.
- CI and release gates covering Maven tests, packaging, CLI help, offline
  preflight checks, public smoke, and Docker Compose config.

## 0.2.0 Planning

`0.2.0` moves Kuaia from a runnable MVP toward a Connector-ready runtime. The
goal is to make built-in connector boundaries easier to extend, validate, and
document before Kuaia takes on CDC, streaming, DAG execution, or distributed
operation.

Planned focus areas:

- MySQL batch source for one bounded JDBC query.
- Stable source configuration validation for file, Postgres, and MySQL
  pipeline YAML.
- Public MySQL example configuration that can be validated without requiring an
  external service in the default smoke path.
- Connector contribution documentation that explains how to add focused source,
  transform, and sink implementations in the current in-process runtime.
- Release gates that cover Maven tests, packaging, CLI help, offline preflight
  checks, public smoke, Docker Compose config, and MySQL validation coverage.

## Deferred Beyond 0.2.0

The following remain future work and should not block `0.2.0`:

- CDC or streaming sources,
- transform DAGs, joins, branches, or fan-out,
- exactly-once execution,
- dynamic plugin loading or a stable connector SDK,
- Kubernetes or distributed HA operation,
- web UI, RBAC, lineage, or governance features,
- broad connector catalog expansion,
- broad vector database support beyond Qdrant.

## Release Criteria

`0.2.0` is ready when the connector-ready focus areas have either shipped or
been explicitly deferred, and the public validation gate passes:

```bash
mvn -q test
mvn -q package
bin/kuaia help
bin/kuaia examples
bin/kuaia validate -f examples/local-file-to-file.yaml
bin/kuaia validate -f examples/local-jsonl-chunk-to-qdrant.yaml
bin/kuaia validate -f examples/postgres-to-qdrant.yaml
bin/kuaia validate -f examples/mysql-to-qdrant.yaml
make public-mvp-smoke
docker compose config
git diff --check
```
