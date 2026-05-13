# Examples

These examples are small local pipelines for the public MVP contract. Run them
from the repository root with `bin/kuaia`. For the current product boundary,
read [`product-scope.md`](product-scope.md) first.

## Recommended MVP Path

Run the no-service smoke check first:

```bash
bin/kuaia examples
make public-mvp-smoke
```

It validates these public MVP paths in an isolated `.kuaia/public-mvp-smoke`
work directory:

- CSV source through `select` and `rename` transforms into a local CSV file,
- JSONL source through `trim` and `filter` transforms into a local JSONL file,
- CSV source through `mock-embedding` into the mock vector sink,
- JSONL source through `trim`, `filter`, and `mock-embedding` into the mock
  vector sink,
- JSONL source through `trim`, `filter`, `chunk`, and `mock-embedding` into the
  mock vector sink,
- malformed CSV handling with `errorPolicy.mode: skip-bad-records`.

After that, run individual examples below when you want to inspect one pipeline
at a time. Qdrant, Postgres, and OpenAI-compatible examples require external
services or credentials and are not part of the default smoke.

## Local File To Console

```bash
bin/kuaia run -f examples/local-file-to-console.yaml
```

Reads `examples/data/users.csv` and prints each row to stdout. This is the
smallest source-to-sink smoke test.

## Local Transform To Console

```bash
bin/kuaia run -f examples/local-file-transform-to-console.yaml
```

Reads the same CSV input, applies the documented `select` and `rename`
transforms, and prints transformed rows.

## Local File To File

```bash
bin/kuaia run -f examples/local-file-to-file.yaml
cat .kuaia/output/local-file-to-file.csv
```

Writes a deterministic CSV file:

```text
id,name
1,Alice
2,Bob
```

This is the default Docker quickstart example because the output is easy to
inspect.

## Local JSONL To File

```bash
bin/kuaia run -f examples/local-jsonl-to-file.yaml
cat .kuaia/output/local-jsonl-to-file.jsonl
```

Reads `examples/data/documents.jsonl`, selects `id` and `content`, trims text,
filters empty or too-short `content` values, and writes deterministic JSON Lines
output:

```json
{"id":1,"content":"Alpha"}
{"id":2,"content":"Beta"}
```

This is the no-service path for checking JSONL cleanup and length filtering
before embedding, chunking, or writing to a vector sink.

## Skip Bad Records

```bash
bin/kuaia run -f examples/local-file-skip-bad-records.yaml
```

Reads `examples/data/users-with-bad-row.csv` with
`errorPolicy.mode: skip-bad-records`. Malformed rows are reported, counted as
failed records in the run summary, and consumed by the checkpoint so reruns do
not process the same bad records again.

## Mock Vector Pipeline

```bash
bin/kuaia run -f examples/local-file-to-vector.yaml
```

Reads `examples/data/documents.csv`, creates a deterministic local embedding
with `mock-embedding`, and prints mock vector sink summaries. It does not call an
external model or vector database.

## JSONL Mock Vector Pipeline

```bash
bin/kuaia run -f examples/local-jsonl-to-vector.yaml
```

Reads `examples/data/documents.jsonl`, trims and filters empty `content` values,
creates deterministic local embeddings, and prints mock vector sink summaries.
This is useful for document or event data that is already stored as JSON Lines.

## JSONL Chunked Vector Pipeline

```bash
bin/kuaia run -f examples/local-jsonl-chunk-to-vector.yaml
```

Reads `examples/data/articles.jsonl`, trims and filters empty `content` values,
splits each remaining `content` field into character-based chunks, creates
deterministic local embeddings for each chunk, and prints mock vector sink
summaries. It is the recommended no-service path for checking document chunking
before using a real embedding provider or vector database.

## OpenAI-Compatible Embedding Pipeline

```bash
export OPENAI_API_KEY=...
bin/kuaia run -f examples/local-file-to-openai-compatible-vector.yaml
```

Reads `examples/data/documents.csv`, calls an OpenAI-compatible embeddings API,
and writes the resulting vectors to the mock vector sink. The example requires a
real API key in `OPENAI_API_KEY`; it is not part of the default automated smoke
tests.

## Qdrant Vector Sink

Start Qdrant:

```bash
docker compose -f docker-compose.qdrant.yml up -d
```

Create the example collection:

```bash
curl -X PUT http://localhost:6333/collections/kuaia_docs \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
```

Run the local file-to-Qdrant pipeline:

```bash
bin/kuaia run -f examples/local-file-to-qdrant.yaml
```

The example reads `examples/data/documents.csv`, creates deterministic mock
embeddings, and upserts points into Qdrant collection `kuaia_docs`. It is not
part of default automated tests because it requires a running Qdrant service.

For chunked JSONL documents, create the chunk collection:

```bash
curl -X PUT http://localhost:6333/collections/kuaia_article_chunks \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
```

Then run:

```bash
bin/kuaia run -f examples/local-jsonl-chunk-to-qdrant.yaml
```

This example reads `examples/data/articles.jsonl`, trims and filters empty
`content` values, splits each remaining document into chunks, generates
deterministic mock embeddings, and writes Qdrant point ids as
`id * 1000000 + chunk_index` so chunks from the same document do not overwrite
each other. It also sets
`dropInput: true` and `includeOffsets: true` so Qdrant payloads keep `chunk`,
`chunk_index`, `chunk_start`, and `chunk_end` without repeating the full source
document text on every point.

## Postgres To Qdrant

Start Postgres and Qdrant:

```bash
docker compose -f docker-compose.postgres.yml -f docker-compose.qdrant.yml up -d
```

Create the example Qdrant collection:

```bash
curl -X PUT http://localhost:6333/collections/kuaia_pg_docs \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
```

Run the batch Postgres-to-Qdrant pipeline:

```bash
export KUAIA_POSTGRES_USER=kuaia
export KUAIA_POSTGRES_PASSWORD=kuaia
bin/kuaia run -f examples/postgres-to-qdrant.yaml
```

The example reads rows from the `documents` table initialized by
`examples/postgres/init/01-documents.sql`, creates deterministic mock
embeddings, and upserts points into Qdrant collection `kuaia_pg_docs`. It is not
part of default automated tests because it requires running Postgres and Qdrant
services.

## Docker Quickstart

```bash
docker compose up --build
```

Compose builds the packaged runtime and runs `examples/local-file-to-file.yaml`
inside the container. The pipeline writes `.kuaia/output/local-file-to-file.csv`
inside `/opt/kuaia`, backed by the `kuaia-state` volume.

## Cleanup

Local runs may create `.kuaia/` state and output directories:

```bash
make clean-state
```
