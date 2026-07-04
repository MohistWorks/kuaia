# Roadmap

Kuaia is a checkpoint-aware AI-ready data pipeline runtime with two execution
modes: the original local in-process runner, and a distributed
coordinator/worker engine with multi-coordinator high availability. The
`0.1.x` line proved the public MVP: local YAML pipelines can read bounded
data, clean and vectorize rows, write to files or Qdrant, resume from local
checkpoints, and expose deterministic validation and run summaries. The
`0.3.x` track added the distributed engine described below (see
`docs/distributed-quickstart.md` for how to run it).

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

## 0.2.0 release-ready

`0.2.0` moves Kuaia from a runnable MVP toward a Connector-ready runtime. The
release-ready scope makes built-in connector boundaries easier to extend,
validate, and document before Kuaia takes on CDC, streaming, DAG execution, or
distributed operation.

Release-ready focus areas:

- MySQL batch source for one bounded JDBC query.
- Stable source configuration validation for file, Postgres, and MySQL
  pipeline YAML.
- Public MySQL example configuration that can be validated without requiring an
  external service in the default smoke path.
- Connector contribution documentation that explains how to add focused source,
  transform, and sink implementations in the current in-process runtime.
- Release gates that cover Maven tests, packaging, CLI help, offline preflight
  checks, public smoke, Docker Compose config, and MySQL validation coverage.

## 0.2.x Roadmap

The `0.2.x` line should turn the connector-ready runtime into useful AI data
pipeline coverage. The `0.2.1` scope shipped on 2026-05-18, the `0.2.2`
scope shipped on 2026-05-18, and the `0.2.3` scope shipped on 2026-05-19 as
the final planned `0.2.x` release.

`0.2.1` shipped scope:

- Connector e2e gate and release-gate wrapper that prove file, Postgres, MySQL,
  DuckDB, document-directory, and Qdrant paths with real local or Docker-backed
  runs through `make e2e` and `make release-gate`.
- DuckDB batch source for local SQL over files, including common CSV, JSON, and
  Parquet read paths exposed through DuckDB queries.
- Local document-directory source for `.txt`, `.md`, and `.markdown` inputs,
  including a document-directory-to-Qdrant example and e2e case.

`0.2.2` shipped scope:

- `0.2.2`: add an S3-compatible object-storage source with MinIO-backed e2e
  coverage for common RAG ingestion inputs.

`0.2.3` final `0.2.x` shipped scope:

- pgvector sink for teams that use Postgres as both the source database and
  vector store.
- Milvus vector sink with Docker-backed e2e coverage after Qdrant and pgvector
  e2e coverage is stable.
- OpenAI-compatible embedding provider e2e coverage using a local fake
  embedding service instead of a real hosted provider.
- Connector contract tests for built-in source and sink implementations.
- Documentation and release-gate alignment for the final `0.2.x` connector
  coverage.

## 0.3.x shipped: execution hardening and the distributed engine

The `0.3.x` track started from the finalized 0.2.x feature set, hardened the
architecture, and then built out distributed operation end to end.

Architectural hardening (shipped):

- Java 21 project baseline across Maven, CI, Docker, and contributor setup.
- Connector runtime interfaces and implementations extracted from
  `kuaia-engine` into the dedicated `kuaia-connectors` module.
- Reduced per-row overhead on the batch ingestion path: allocation-free
  `BinaryRow` field access via `VarHandle` and unboxed per-row sequence
  tracking, with the row byte layout unchanged.
- Connector execution and contribution docs for contributor onboarding.

Distributed execution engine (shipped):

- Coordinator/worker runtime over gRPC bidirectional streams: task planner,
  job-level state aggregation, a lease-based dispatch loop with
  compare-and-set task ownership, worker-side split execution, backpressure
  signaling, and capped task retries so failing tasks finalize.
- Persistent coordinator state in RocksDB with recovery of planned and
  in-flight tasks across coordinator restarts.
- Operations surface: `kuaia coordinator` and `kuaia worker` launch the
  processes; `kuaia submit` sends a pipeline to a running coordinator and
  `kuaia status` polls job progress over the same gRPC API
  (SubmitJob/GetJobStatus/ListJobs).
- Multi-coordinator high availability: all job/task/worker state replicated
  through Raft (Apache Ratis) into a RocksDB state machine, dispatch gated on
  the elected leader, and an in-process three-node failover test proving a
  leader crash is survived without losing job state.
- Worker leader auto-discovery: workers take the full coordinator list, probe
  for the current leader via a `HelloAck` handshake, and reconnect with capped
  backoff across leader failover — no operator re-pointing.

The intended AI connector coverage is:

- batch sources: file, Postgres, MySQL, DuckDB, document directory, and
  S3-compatible object storage,
- embedding providers: mock and OpenAI-compatible, with local-provider support
  such as Ollama evaluated after fake-server e2e coverage is stable,
- vector sinks: Qdrant, pgvector, and Milvus,
- debug sinks: console, file, and mock-vector.

## Deferred

The following remain future work:

- CDC or streaming sources,
- transform DAGs, joins, branches, or fan-out,
- exactly-once execution,
- dynamic Raft cluster membership (adding or removing coordinator nodes at
  runtime) and dynamic coordinator-list changes on workers,
- worker-side load balancing across coordinators,
- Kubernetes operator or managed deployment tooling,
- dynamic plugin loading or a stable connector SDK,
- web UI, RBAC, lineage, or governance features,
- broad connector catalog expansion,
- broad vector database support beyond the current Qdrant, pgvector, and Milvus
  set.

## Release Criteria

Each `0.2.x` release is ready to tag when its connector focus area has shipped
and the public validation gate passes:

```bash
make release-gate
```

The release gate currently runs:

```bash
mvn -q test
mvn -q package
bin/kuaia help
bin/kuaia examples
bin/kuaia validate -f examples/local-file-to-file.yaml
bin/kuaia validate -f examples/local-jsonl-chunk-to-qdrant.yaml
bin/kuaia validate -f examples/document-directory-to-qdrant.yaml
bin/kuaia validate -f examples/duckdb-csv-to-qdrant.yaml
bin/kuaia validate -f examples/s3-docs-to-qdrant.yaml
bin/kuaia validate -f examples/local-file-to-openai-compatible-vector.yaml
bin/kuaia validate -f examples/local-file-to-milvus.yaml
bin/kuaia validate -f examples/postgres-to-qdrant.yaml
bin/kuaia validate -f examples/postgres-to-pgvector.yaml
bin/kuaia validate -f examples/mysql-to-qdrant.yaml
make public-mvp-smoke
make e2e
docker compose config
docker compose -f docker-compose.postgres.yml config
docker compose -f docker-compose.mysql.yml config
git diff --check
```

After `0.2.3`, do not cut a new version until at least five public changes have
landed since the previous release.
