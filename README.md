# Kuaia

[![CI](https://github.com/gnehil/kuaia/actions/workflows/ci.yml/badge.svg)](https://github.com/gnehil/kuaia/actions/workflows/ci.yml)

Kuaia is an experimental Java runtime for local, AI-ready data pipelines. It
runs declarative YAML pipelines that read bounded data, transform rows, create
embeddings, write to files or vector sinks, and resume from checkpoints.

The long-term direction is an AI-ready DataOps engine. The public repository is
currently an MVP focused on local, checkpoint-aware batch execution.

## What You Can Use Today

Kuaia is useful when you want to:

- run a local CSV, JSONL, document directory, S3-compatible object storage,
  DuckDB, Postgres, or MySQL batch pipeline from YAML,
- transform records through a typed `BinaryRow` model,
- trim and filter empty text before embedding or chunking,
- split JSONL document text into embed-ready chunks,
- ingest local `.txt`, `.md`, and `.markdown` document directories while
  preserving relative paths,
- ingest text-like objects from S3-compatible storage such as MinIO while
  preserving object keys,
- import small FAQ JSONL datasets while preserving question and answer fields,
- generate mock or OpenAI-compatible embeddings,
- write to console, local CSV or JSONL files, mock vector output, Qdrant, or
  pgvector,
- resume local runs from checkpoint state,
- run a local batch benchmark with split, checkpoint, embedding, and sink
  counters,
- inspect connector and runtime extension boundaries.

Kuaia is not yet a production ETL platform. The MVP does not provide CDC,
streaming DAG execution, joins, fan-out, production installers, a web UI, RBAC,
lineage, Kubernetes operation, or exactly-once guarantees. The current target is
at-least-once style processing with idempotent sinks.

For the detailed product boundary, read
[`docs/product-scope.md`](docs/product-scope.md). Public release notes are in
[`CHANGELOG.md`](CHANGELOG.md).

## Quick Start

Requirements:

- JDK 21 or newer
- Maven 3.8+
- Docker with Compose support, optional

Run the CLI and the default local example:

```bash
bin/kuaia help
bin/kuaia examples
bin/kuaia validate -f examples/local-file-to-file.yaml
bin/kuaia run -f examples/local-file-to-file.yaml
bin/kuaia run -f examples/local-file-to-file.yaml --summary-json .kuaia/output/local-file-to-file-summary.json
bin/kuaia run -f examples/local-quoted-csv-to-file.yaml
bin/kuaia run -f examples/local-jsonl-to-file.yaml
bin/kuaia run -f examples/local-faq-jsonl-to-vector.yaml
cat .kuaia/output/local-file-to-file.csv
cat .kuaia/output/local-file-to-file-summary.json
cat .kuaia/output/local-quoted-csv-to-file.csv
cat .kuaia/output/local-jsonl-to-file.jsonl
```

The example reads `examples/data/users.csv` and writes a deterministic CSV file
under `.kuaia/output/local-file-to-file.csv`. The quoted CSV example reads
`examples/data/quoted-documents.csv` and preserves commas, escaped quotes, and
line breaks through `.kuaia/output/local-quoted-csv-to-file.csv`. The JSONL file
example reads `examples/data/documents.jsonl`, trims, lowercases, and filters
`content`, and writes `.kuaia/output/local-jsonl-to-file.jsonl`.
The FAQ vector example reads `examples/data/faq.jsonl`, trims question and
answer fields, filters empty values, and emits deterministic mock vectors.

Use `kuaia validate -f <pipeline.yaml>` to check a pipeline before running it.
For file and document-directory sources, validation checks the source row type,
transform field compatibility, and sink field compatibility without writing
output or checkpoint state. For S3, DuckDB, Postgres, and MySQL sources,
validation parses connector configuration without connecting to the source, so
row-type checks are deferred until run time.

Validate the public MVP paths without external services:

```bash
make public-mvp-smoke
```

This smoke test runs file-to-file, quoted CSV, JSONL cleanup, mock-vector,
FAQ import, and bad-record handling examples in an isolated local state
directory.

Run the Docker-backed connector e2e gate when changing connector behavior:

```bash
make e2e
```

This starts local Postgres, MySQL, Qdrant, Milvus, MinIO, and fake
OpenAI-compatible embedding services with Docker Compose, runs file-to-Qdrant,
document-directory-to-Qdrant, DuckDB-to-Qdrant, S3-to-Qdrant,
file-to-OpenAI-compatible-embedding, file-to-Milvus, Postgres-to-Qdrant,
Postgres-to-pgvector, and MySQL-to-Qdrant pipelines, checks run summaries, and
tears the services down.

To iterate on one connector path, pass a case name:

```bash
make e2e CASE=file-qdrant
make e2e CASE=document-directory-qdrant
make e2e CASE=duckdb-qdrant
make e2e CASE=s3-qdrant
make e2e CASE=file-openai-compatible-vector
make e2e CASE=file-milvus
make e2e CASE=postgres-qdrant
make e2e CASE=postgres-pgvector
make e2e CASE=mysql-qdrant
./scripts/connector-e2e-smoke.sh --list
```

Single-case runs only start the services needed by that path.

## Build A Packaged Runtime

```bash
mvn -q package
VERSION=$(sed -n 's/.*<version>\(.*\)<\/version>.*/\1/p' pom.xml | head -n 1)
java -jar "kuaia-engine/target/kuaia-engine-${VERSION}-cli.jar" help
```

After packaging, `bin/kuaia` uses the packaged jar automatically. Before
packaging, the script falls back to Maven so examples can run from a fresh
checkout.

Useful Make targets:

```bash
make test
make public-mvp-smoke
make e2e
make release-gate
make run-vector
make benchmark
make clean-state
```

## Run With Docker

```bash
docker compose up --build
```

Docker Compose builds the packaged runtime and runs
`examples/local-file-to-file.yaml`. The output path inside the container is
`.kuaia/output/local-file-to-file.csv`, backed by the `kuaia-state` Compose
volume. For local host output, run the same YAML with `bin/kuaia`.

## Example Pipeline

A Kuaia pipeline is a small YAML document:

```yaml
name: local-file-to-vector
source:
  type: file
  path: data/documents.csv
  format: csv
transforms:
  - type: select
    fields: [id, content]
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: mock-vector
checkpoint:
  stateDir: .kuaia/state/local-file-to-vector
```

Run it with:

```bash
bin/kuaia run -f examples/local-file-to-vector.yaml
```

Declarative runs print a summary with rows read, rows written, failed records,
checkpoint sequence, task state, source splits, sink batches, and duration.
Add `--summary-json <path>` to write the same counters as machine-readable JSON
for scripts and CI checks. Failed declarative runs include a stable stage
prefix such as `Source stage failed:`, `Transform stage failed:`, or
`Sink stage failed:` before the underlying error. Checkpoint failures are
reported with `Checkpoint stage failed:`.

The full YAML contract is documented in
[`docs/pipeline-yaml.md`](docs/pipeline-yaml.md).

## Local Benchmark

Run the built-in local batch benchmark:

```bash
bin/kuaia benchmark
```

It generates CSV input and writes JSON counters to
`target/kuaia-benchmark/local-pipeline-batch.json`. By default it runs batch
sizes `1`, `8`, `32`, and `128`. For larger runs or split behavior checks:

```bash
bin/kuaia benchmark --rows 10000 --max-rows-per-split 1000
```

To compare specific transform and sink batch sizes:

```bash
bin/kuaia benchmark --rows 10000 --batch-sizes 16,64,256
```

To write spreadsheet-friendly CSV instead of JSON:

```bash
bin/kuaia benchmark --format csv --output target/kuaia-benchmark/local-pipeline-batch.csv
```

## Examples

List available examples:

```bash
bin/kuaia examples
```

Common local examples:

```bash
bin/kuaia run -f examples/local-file-to-console.yaml
bin/kuaia run -f examples/local-file-transform-to-console.yaml
bin/kuaia run -f examples/local-file-skip-bad-records.yaml
bin/kuaia run -f examples/local-quoted-csv-to-file.yaml
bin/kuaia run -f examples/local-file-to-vector.yaml
bin/kuaia run -f examples/local-jsonl-to-vector.yaml
bin/kuaia run -f examples/local-jsonl-chunk-to-vector.yaml
bin/kuaia run -f examples/local-faq-jsonl-to-vector.yaml
```

OpenAI-compatible embedding example:

```bash
export OPENAI_API_KEY=...
bin/kuaia run -f examples/local-file-to-openai-compatible-vector.yaml
```

The OpenAI-compatible embedding transform supports `batchSize` for batched
array input requests, `timeoutMs` for HTTP connect/read timeout control, and
stable validation of batch response counts and embedding indexes.
For a no-credential e2e proof, run `make e2e CASE=file-openai-compatible-vector`;
the e2e case starts a local fake OpenAI-compatible embedding service with
Docker.

Qdrant example:

```bash
docker compose -f docker-compose.qdrant.yml up -d
curl -X PUT http://localhost:6333/collections/kuaia_docs \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
curl -X PUT http://localhost:6333/collections/kuaia_article_chunks \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
bin/kuaia run -f examples/local-file-to-qdrant.yaml
bin/kuaia run -f examples/local-jsonl-chunk-to-qdrant.yaml
```

The Qdrant examples use `payloadFields` to keep vector payloads predictable. The
chunked example combines stable chunk point ids with selected payload metadata,
so it omits repeated source text while keeping chunk character offsets.

Postgres-to-Qdrant example:

```bash
docker compose -f docker-compose.postgres.yml -f docker-compose.qdrant.yml up -d
curl -X PUT http://localhost:6333/collections/kuaia_pg_docs \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
export KUAIA_POSTGRES_USER=kuaia
export KUAIA_POSTGRES_PASSWORD=kuaia
bin/kuaia run -f examples/postgres-to-qdrant.yaml
```

Postgres-to-pgvector example:

```bash
docker compose -f docker-compose.postgres.yml up -d
export KUAIA_POSTGRES_USER=kuaia
export KUAIA_POSTGRES_PASSWORD=kuaia
bin/kuaia run -f examples/postgres-to-pgvector.yaml
```

The pgvector example writes into the pre-created `document_vectors` table from
`examples/postgres/init/01-documents.sql`; Kuaia does not create target tables
automatically.

Local file-to-Milvus example:

```bash
export KUAIA_MILVUS_TOKEN=root:Milvus
bin/kuaia run -f examples/local-file-to-milvus.yaml
```

The Milvus example writes into a pre-created collection named `kuaia_docs`.
Kuaia does not create target collections automatically. For an automated local
proof that starts Milvus with Docker, run `make e2e CASE=file-milvus`.

DuckDB-to-Qdrant example:

```bash
docker compose -f docker-compose.qdrant.yml up -d
curl -X PUT http://localhost:6333/collections/kuaia_duckdb_docs \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
bin/kuaia run -f examples/duckdb-csv-to-qdrant.yaml
```

S3-compatible object storage to Qdrant example:

```bash
export KUAIA_S3_ACCESS_KEY=...
export KUAIA_S3_SECRET_KEY=...
bin/kuaia run -f examples/s3-docs-to-qdrant.yaml
```

The example expects a bucket named `kuaia-docs` with text-like objects under
the `docs/` prefix, plus a running Qdrant collection named `kuaia_s3_docs`.
For an automated local proof with MinIO and Qdrant, run
`make e2e CASE=s3-qdrant`.

MySQL-to-Qdrant example:

```bash
docker compose -f docker-compose.mysql.yml -f docker-compose.qdrant.yml up -d
curl -X PUT http://localhost:6333/collections/kuaia_mysql_docs \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
export KUAIA_MYSQL_USER=kuaia
export KUAIA_MYSQL_PASSWORD=kuaia
bin/kuaia run -f examples/mysql-to-qdrant.yaml
```

Read [`docs/examples.md`](docs/examples.md) for the full example catalog and
service requirements.

## Supported MVP Surface

| Area | Current support |
| --- | --- |
| Sources | `file` CSV and JSONL, `document-directory`, `s3`, batch `duckdb`, `postgres`, and `mysql` queries |
| Transforms | `select`, `rename`, `trim`, `lowercase`, `replace`, `filter` (`not-empty`, `min-length`, `contains`, `starts-with`, `ends-with`, `equals`, `not-equals`, `greater-than`, `greater-than-or-equal`, `less-than`, `less-than-or-equal`), `chunk`, `mock-embedding`, OpenAI-compatible `embedding` |
| Sinks | `console`, CSV/JSONL `file`, `mock-vector`, `qdrant`, `pgvector`, `milvus` |
| Runtime | Linear batch pipeline, preflight validation, checkpoint resume, bad-record skip mode |
| State | Local checkpoint state, in-memory and RocksDB state stores |
| Extension points | Source, transform, sink, split reader, and batch writer boundaries |

For stricter local file runs, set `KUAIA_RESTRICT_LOCAL_PATHS=true` to reject
YAML paths outside the YAML directory or repository `.kuaia/`.

## Documentation

- [`docs/README.md`](docs/README.md): public documentation index
- [`docs/product-scope.md`](docs/product-scope.md): current product definition
  and non-goals
- [`docs/roadmap.md`](docs/roadmap.md): current release status, validation
  gate, and deferred work
- [`docs/pipeline-yaml.md`](docs/pipeline-yaml.md): YAML pipeline contract
- [`docs/examples.md`](docs/examples.md): runnable example catalog
- [`docs/distributed-quickstart.md`](docs/distributed-quickstart.md): run a
  coordinator + worker across processes
- [`docs/ha-quickstart.md`](docs/ha-quickstart.md): highly available 3-node
  coordinator cluster with failover and runtime membership
- [`docs/connector-development.md`](docs/connector-development.md): connector
  extension notes
- [`docs/release-checklist.md`](docs/release-checklist.md): open-source MVP
  release checks
- [`CHANGELOG.md`](CHANGELOG.md): public release notes

## Contributing

Contributions should stay aligned with the current MVP scope and keep public
user-facing documentation in `README.md` or `docs/`.

Before submitting runtime or connector changes, run the release gate:

```bash
make release-gate
```

It includes Maven tests, packaging, CLI validation, public smoke coverage,
Docker-backed connector e2e coverage, Compose config validation, and
`git diff --check`.

For a focused local check while iterating, run:

```bash
mvn -q test
mvn -q package
make e2e
make public-mvp-smoke
git diff --check
```

For documentation-only changes, run the relevant focused tests plus
`git diff --check`. Contributor workflow details are in
[`CONTRIBUTING.md`](CONTRIBUTING.md).

## Repository Layout

- `kuaia-common`: shared data model, connector APIs, protobuf contracts, and
  common utilities
- `kuaia-engine`: local execution, worker/coordinator runtime components, state
  stores, Raft integration, and runnable demos
- `examples`: runnable YAML examples and sample data
- `docs`: public user and contributor documentation
- `scripts`: local developer and public MVP smoke checks

Internal design notes are intentionally not part of the public documentation
tree.

## Security

Please report suspected vulnerabilities through GitHub Security Advisories. See
[`SECURITY.md`](SECURITY.md) for the current MVP security policy.

## License

Kuaia is licensed under the Apache License, Version 2.0. See [`LICENSE`](LICENSE).
