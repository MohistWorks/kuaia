# Pipeline YAML

Kuaia's current public pipeline YAML is a small MVP contract for local examples.
It is intentionally not a general YAML dialect or production job spec. For the
current product boundary, see [`product-scope.md`](product-scope.md).

## Shape

```yaml
name: local-file-to-file
source:
  type: file
  path: data/users.csv
  format: csv
sink:
  type: file
  path: ../.kuaia/output/local-file-to-file.csv
  format: csv
  mode: overwrite
checkpoint:
  stateDir: .kuaia/state/local-file-to-file
```

Required top-level fields:

- `name`
- `source`
- `sink`

Optional top-level fields:

- `transforms`
- `checkpoint`
- `errorPolicy`

## Source

### file

```yaml
source:
  type: file
  path: data/users.csv
  format: csv
  maxRowsPerSplit: 10000
```

Fields:

- `type`: must be `file`
- `path`: a storage URI whose scheme selects the backend (see the scheme table
  below). A bare path or a `file://` URI reads the local filesystem, and
  relative local paths are resolved from the YAML file directory. For
  `format: document` the path may be a directory/prefix or a single object; for
  `format: csv` and `format: jsonl` it points at a single object.
- `format`: must be `csv`, `jsonl`, or `document`
- `documentType`: optional document selector for `format: document`. Must be
  `auto`, `text`, `markdown`, or `pdf` when configured and defaults to `auto`.
  Rejected for the `csv` and `jsonl` formats and for non-file source types.
- `maxRowsPerSplit`: optional internal source split size for the `csv` and
  `jsonl` formats. Defaults to `10000` and must be a positive integer when
  configured. Rejected for `format: document`.

The `path` scheme selects the storage backend behind the `file` source:

| Scheme | Backend | Notes |
| --- | --- | --- |
| bare path or `file://` | local filesystem | relative paths resolve from the YAML directory |
| `s3://bucket/key` | S3-compatible object storage | bucket and prefix live in the URI; requires the `endpoint`, `region`, `accessKeyEnv`, and `secretKeyEnv` sibling fields (optional `pathStyleAccess`) |
| `hdfs://` | planned, not yet supported | loading fails with `source.path storage scheme hdfs:// is not yet supported` |

Any other scheme fails to load with `source.path storage scheme <scheme>:// is
not supported`.

For an `s3://` path, set these sibling fields next to `path`:

- `endpoint`: S3-compatible endpoint URL, for example `http://localhost:9000`
- `region`: signing region, for example `us-east-1`
- `accessKeyEnv`: environment variable containing the access key
- `secretKeyEnv`: environment variable containing the secret key
- `pathStyleAccess`: optional boolean. Defaults to `true`, which is the common
  setting for MinIO and other S3-compatible endpoints.

These sibling fields are only accepted for `s3://` paths; a local `path` that
carries `endpoint`, `region`, `accessKeyEnv`, `secretKeyEnv`, `pathStyleAccess`,
`bucket`, or `prefix` fails to load. The `bucket` and `prefix` keys are not
accepted for `source.type: file` at all — fold them into the
`s3://bucket/prefix/` URI.

An `s3://` path supports the same formats as a local path: `csv` and `jsonl`
tables plus `document` (including PDF text extraction). A directory/prefix path
for `format: document` MUST end in `/`; the trailing slash is load-bearing
because it marks the URI as a prefix (corpus) rather than a single object,
while a `csv` or `jsonl` `s3://` path points at a single object with no trailing
slash.

`validate` defers remote (`s3://`) sources: it parses the YAML and connector
options without connecting to object storage, so transform and sink row-type
checks against a remote source run at `run` time. Local `file` sources are
opened during `validate`.

CSV rules:

- first line is the header,
- empty lines are skipped,
- every data row must have the same number of columns as the header,
- a field named `id` is parsed as `LONG`,
- other fields are parsed as `STRING`,
- quoted CSV fields are supported for commas, escaped quotes, and line breaks
  inside quoted values.

JSONL rules:

- each non-empty line must be one JSON object,
- the row schema is inferred from the first non-empty JSON object,
- field order follows the first JSON object's field order,
- every later JSON object must contain the same fields and no extra fields,
- a field named `id` or a field inferred from an integer value is parsed as
  `LONG`,
- other non-null scalar fields are parsed as `STRING`,
- JSON arrays, nested objects, and null values are not supported in this MVP,
- malformed JSONL rows are source row errors and can be skipped with
  `errorPolicy.mode: skip-bad-records`.

Document rules:

```yaml
source:
  type: file
  path: data/docs
  format: document
  documentType: auto
```

`format: document` recursively reads local documents and emits one row per
supported file. It is intended for small local RAG ingestion inputs where
relative source paths should be kept as metadata.

- `path` may be a directory or a single document file; a single file is read
  as a one-document corpus,
- `documentType: auto` selects `.txt`, `.md`, `.markdown`, and `.pdf` files;
  `text` selects `.txt`; `markdown` selects `.md` and `.markdown`; `pdf`
  selects `.pdf`,
- unsupported files are ignored,
- documents are processed in stable relative-path order,
- output fields are fixed as `id LONG`, `path STRING`, and `content STRING`,
- `id` is the 1-based sequence id in sorted document order,
- `path` is the slash-separated relative path under `source.path`, or the file
  name for a single-file corpus,
- `content` is the UTF-8 file content for text documents and the extracted
  text for `.pdf` documents,
- PDF extraction reads embedded text with `"\n"` line separators on every
  platform; there is no OCR, so scanned PDFs produce rows with blank
  (whitespace-only) `content` that the trimming `op: not-empty` `filter`
  transform drops,
- corrupt or encrypted PDFs are source row errors and can be skipped with
  `errorPolicy.mode: skip-bad-records`.

Migration note: `source.type: document-directory` has been removed. Loading a
pipeline that still uses it fails with `source.type document-directory has
been replaced by source.type: file with format: document`. Change the source
to `type: file` with `format: document` and keep the same `path`.

Checkpoint state is keyed by the pipeline `name`. When migrating a
checkpointed pipeline, start with a fresh `checkpoint.stateDir` or a new
`name`; resuming against the old state can no-op, silently skip `.pdf` files
newly included by `documentType: auto`, or duplicate the tail document.

### s3 migration

The standalone `source.type: s3` source has been removed. S3-compatible object
storage is now read through `source.type: file` with an `s3://` path, so it
flows through the same `FileSource`/`DocumentSource` code as local files and
gains csv/jsonl table parsing and document (including PDF) extraction.

Loading a pipeline that still uses `source.type: s3` fails with `source.type s3
has been replaced by source.type: file with an s3:// path`. Migrate the source
block by folding the old `bucket` and `prefix` into the `s3://bucket/prefix/`
URI and keeping the endpoint, region, and credential fields as siblings of
`path`:

```yaml
# before
source:
  type: s3
  endpoint: http://localhost:9000
  region: us-east-1
  bucket: kuaia-docs
  prefix: docs/
  accessKeyEnv: KUAIA_S3_ACCESS_KEY
  secretKeyEnv: KUAIA_S3_SECRET_KEY
  pathStyleAccess: true

# after
source:
  type: file
  path: s3://kuaia-docs/docs/
  format: document
  endpoint: http://localhost:9000
  region: us-east-1
  accessKeyEnv: KUAIA_S3_ACCESS_KEY
  secretKeyEnv: KUAIA_S3_SECRET_KEY
  pathStyleAccess: true
```

The middle document column is now `path` (was `key`), so update sink
`payloadFields` from `[id, key, content]` to `[id, path, content]`, and note
the `path` value is the object key relative to the prefix (e.g. `a.pdf`),
matching local documents, not the full key (`docs/a.pdf`) the old `key` column
held. The
`bucket` and `prefix` keys are rejected; the trailing slash on
`s3://kuaia-docs/docs/` marks it as a prefix (corpus) rather than a single
object.

### postgres

```yaml
source:
  type: postgres
  url: jdbc:postgresql://localhost:5432/kuaia
  userEnv: KUAIA_POSTGRES_USER
  passwordEnv: KUAIA_POSTGRES_PASSWORD
  query: select id, content from documents order by id
  fetchSize: 1000
```

`source.type: postgres` runs one batch JDBC query and streams the result rows
through the local pipeline. It is not a CDC source.

Fields:

- `type`: must be `postgres`
- `url`: PostgreSQL JDBC URL
- `userEnv`: environment variable containing the database user
- `passwordEnv`: environment variable containing the database password
- `query`: SQL query to execute
- `fetchSize`: optional JDBC fetch size. Defaults to driver behavior and must
  be a positive integer when configured.

Postgres type rules:

- integer-like columns are exposed as `LONG`,
- all other columns are exposed as `STRING`,
- result rows use 1-based source sequence ids in query order,
- checkpoint reruns skip result rows at or before the last checkpoint sequence.

Credentials are read from the environment at runtime and are never stored in
YAML. Query failures and missing credential environment variables are fatal
source errors.

### mysql

```yaml
source:
  type: mysql
  url: jdbc:mysql://localhost:3306/kuaia
  userEnv: KUAIA_MYSQL_USER
  passwordEnv: KUAIA_MYSQL_PASSWORD
  query: select id, content from documents order by id
  fetchSize: 1000
```

`source.type: mysql` runs one batch JDBC query and streams the result rows
through the local pipeline. It is not a CDC source.

Fields:

- `type`: must be `mysql`
- `url`: MySQL JDBC URL
- `userEnv`: environment variable containing the database user
- `passwordEnv`: environment variable containing the database password
- `query`: SQL query to execute
- `fetchSize`: optional JDBC fetch size. Defaults to driver behavior and must
  be a positive integer when configured.

MySQL type rules:

- integer-like columns are exposed as `LONG`,
- all other columns are exposed as `STRING`,
- result rows use 1-based source sequence ids in query order,
- checkpoint reruns skip result rows at or before the last checkpoint sequence.

Credentials are read from the environment at runtime and are never stored in
YAML. Query failures and missing credential environment variables are fatal
source errors.

### duckdb

```yaml
source:
  type: duckdb
  query: select id, content from read_csv_auto('examples/data/documents.csv') order by id
  fetchSize: 1000
```

`source.type: duckdb` runs one bounded DuckDB SQL query in an in-process DuckDB
database. It is intended for local SQL over files, including CSV, JSON, and
Parquet paths that DuckDB can read from SQL.

Fields:

- `type`: must be `duckdb`
- `query`: SQL query to execute
- `url`: optional DuckDB JDBC URL. Defaults to `jdbc:duckdb:` for an in-memory
  database and must start with `jdbc:duckdb:` when configured.
- `fetchSize`: optional JDBC fetch size. Defaults to driver behavior and must
  be a positive integer when configured.

DuckDB query examples:

```sql
select id, content from read_csv_auto('examples/data/documents.csv') order by id
select id, content from read_json_auto('examples/data/documents.jsonl') order by id
select id, content from read_parquet('target/data/documents.parquet') order by id
```

DuckDB type rules:

- integer-like columns are exposed as `LONG`,
- all other columns are exposed as `STRING`,
- result rows use 1-based source sequence ids in query order,
- checkpoint reruns skip result rows at or before the last checkpoint sequence.

DuckDB sources do not use `userEnv` or `passwordEnv`. Query failures are fatal
source errors.

## Transforms

Transforms are optional and run in YAML order. The pipeline is linear, not a DAG.

### select

```yaml
transforms:
  - type: select
    fields: [id, name]
```

`select` keeps fields in the listed order.

Rules:

- every selected field must exist,
- selected fields must be unique,
- field types and values are preserved.

### rename

```yaml
transforms:
  - type: rename
    from: name
    to: user_name
```

`rename` changes a field name without changing its position, type, or value.

Rules:

- `from` must exist,
- `to` must not duplicate another field name unless it is the same field.

### trim

```yaml
transforms:
  - type: trim
    field: content
```

`trim` removes leading and trailing whitespace from a string field while
preserving the row schema. It is useful before `filter`, `chunk`, or
`embedding` in JSONL/RAG-style pipelines.

Rules:

- `field` must exist and be `STRING`,
- the configured field is updated in place,
- field names, field order, and field types are preserved.

### lowercase

```yaml
transforms:
  - type: lowercase
    field: content
```

`lowercase` normalizes a string field with `Locale.ROOT` while preserving the
row schema. It is useful before case-sensitive `filter`, `chunk`, or
`embedding` stages.

Rules:

- `field` must exist and be `STRING`,
- the configured field is updated in place,
- field names, field order, and field types are preserved.

### replace

```yaml
transforms:
  - type: replace
    field: content
    target: "  "
    replacement: " "
```

`replace` normalizes a string field by replacing every literal `target`
occurrence with `replacement`. It is not a regular-expression transform.

Rules:

- `field` must exist and be `STRING`,
- `target` is required and must not be empty,
- `replacement` is optional and defaults to an empty string,
- the configured field is updated in place,
- field names, field order, and field types are preserved.

### filter

```yaml
transforms:
  - type: filter
    field: content
    op: not-empty
```

`filter` drops rows that do not match the configured predicate while preserving
the row schema.

Supported predicates:

- `op: not-empty` keeps rows whose configured string field contains at least one
  non-whitespace character.
- `op: min-length` keeps rows whose configured string field has at least
  `minLength` non-whitespace characters after trimming for predicate evaluation.
- `op: contains` keeps rows whose configured string field contains the
  configured `value` substring. Matching is case-sensitive.
- `op: starts-with` keeps rows whose configured string field starts with the
  configured `value` prefix. Matching is case-sensitive.
- `op: ends-with` keeps rows whose configured string field ends with the
  configured `value` suffix. Matching is case-sensitive.
- `op: equals` keeps rows whose configured string field exactly equals the
  configured `value`. Matching is case-sensitive.
- `op: not-equals` keeps rows whose configured string field does not exactly
  equal the configured `value`. Matching is case-sensitive.
- `op: greater-than` keeps rows whose configured `LONG` field is greater than
  the configured integer `value`.
- `op: greater-than-or-equal` keeps rows whose configured `LONG` field is
  greater than or equal to the configured integer `value`.
- `op: less-than` keeps rows whose configured `LONG` field is less than the
  configured integer `value`.
- `op: less-than-or-equal` keeps rows whose configured `LONG` field is less
  than or equal to the configured integer `value`.

Example:

```yaml
transforms:
  - type: filter
    field: content
    op: min-length
    minLength: 20
  - type: filter
    field: content
    op: contains
    value: invoice
  - type: filter
    field: content
    op: ends-with
    value: paid
  - type: filter
    field: status
    op: not-equals
    value: archived
  - type: filter
    field: id
    op: greater-than-or-equal
    value: 1000
```

Rules:

- `field` must exist and be `STRING` for string predicates, or `LONG` for
  numeric comparison predicates,
- `op` is required,
- supported `op` values: `not-empty`, `min-length`, `contains`,
  `starts-with`, `ends-with`, `equals`, `not-equals`, `greater-than`,
  `greater-than-or-equal`, `less-than`, `less-than-or-equal`,
- `minLength` is required for `op: min-length` and must be a positive integer,
- `value` is required for `op: contains`, `op: starts-with`, and
  `op: ends-with`, `op: equals`, and `op: not-equals`, and must not be empty,
- `value` is required for numeric comparison predicates and must parse as a
  `LONG`,
- dropped rows are counted as source rows read, not as rows written,
- checkpointed runs still advance the checkpoint after a filtered source batch
  is processed.

### chunk

```yaml
transforms:
  - type: chunk
    input: content
    output: chunk
    chunkSize: 500
    overlap: 50
    dropInput: true
    includeOffsets: true
```

`chunk` splits a string field into character-based text chunks. By default it
preserves the original row fields and appends:

- the configured `output` string field containing the chunk text,
- `chunk_index` as a `LONG`, starting at `0` for each input row.

Optional payload controls:

- `dropInput`: defaults to `false`; when `true`, removes the original input
  text field from output rows. Other source fields are preserved.
- `includeOffsets`: defaults to `false`; when `true`, appends `chunk_start` and
  `chunk_end` as `LONG` character offsets.

Rules:

- `input` must exist and be `STRING`,
- `output` must not already exist,
- `chunk_index` must not already exist,
- `chunk_start` and `chunk_end` must not already exist when `includeOffsets`
  is `true`,
- `chunkSize` is required and must be a positive integer,
- `overlap` is optional, defaults to `0`, and must be smaller than
  `chunkSize`,
- `dropInput` and `includeOffsets` must be `true` or `false` when configured,
- empty input text emits zero output rows,
- chunking is character-based, not token-based.

`chunk` can expand one source row into multiple output rows. Run summaries keep
`rowsRead` as source rows and count `rowsWritten` as emitted output rows.

### mock-embedding

```yaml
transforms:
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
```

`mock-embedding` appends a deterministic `VECTOR` field. It is a local mock for
AI-ready flow testing and does not call an external model.

Rules:

- `input` must exist and be `STRING`,
- `output` must not already exist,
- `dimensions` is optional and defaults to `4`,
- `dimensions` must be a positive integer,
- `batchSize` is optional and defaults to `32`.

The mock vector is deterministic:

```text
vector[i] = inputText.length() + i
```

Implementation note: `mock-embedding` is backed by Kuaia's local `mock`
embedding provider. It remains as a stable offline shortcut; new provider-backed
flows should use the generic `embedding` transform.

### embedding

```yaml
transforms:
  - type: embedding
    provider: openai-compatible
    input: content
    output: embedding
    model: text-embedding-3-small
    apiKeyEnv: OPENAI_API_KEY
    baseUrl: https://api.openai.com/v1
    dimensions: 1536
    timeoutMs: 30000
    batchSize: 32
```

`embedding` appends a `VECTOR` field using a configured embedding provider.

Supported providers:

- `mock`: local deterministic provider, useful for offline tests. It follows the
  same vector rule as `mock-embedding`.
- `openai-compatible`: HTTP provider for APIs that implement the
  OpenAI-compatible `POST /v1/embeddings` contract.

Fields:

- `provider`: required. Supported values are `mock` and `openai-compatible`.
- `input`: required string field to embed.
- `output`: required output vector field name.
- `dimensions`: optional. For `mock`, defaults to `4`. For
  `openai-compatible`, it is omitted from the HTTP request unless configured.
- `batchSize`: optional. Defaults to `32` and must be a positive integer.

Additional `openai-compatible` fields:

- `model`: required model id, for example `text-embedding-3-small`.
- `apiKeyEnv`: required environment variable name that contains the bearer token.
- `baseUrl`: optional, defaults to `https://api.openai.com/v1`. Set it to the
  `/v1` base URL of another OpenAI-compatible service when needed.
- `timeoutMs`: optional request connect/read timeout in milliseconds. Defaults
  to `30000` and must be a positive integer.

Kuaia sends one embedding request per batch. For `openai-compatible`, the
request body contains `input` as an array of strings, `model`, and
`encoding_format: float`; when `dimensions` is configured, it is included as a
positive integer. The API key is read at runtime from `apiKeyEnv` and is never
stored in YAML. The configured `timeoutMs` is applied to both the HTTP connect
timeout and read timeout. Batch responses are mapped by `index` when present,
or by response order when `index` is omitted. Kuaia rejects missing embeddings,
count mismatches, duplicate indexes, and out-of-range indexes.

## Sink

### console

```yaml
sink:
  type: console
```

Prints all output row fields supported by the current console sink.

### file

```yaml
sink:
  type: file
  path: ../.kuaia/output/local-file-to-file.csv
  format: csv
  mode: overwrite
```

Writes output rows as a local CSV or JSONL file.

Fields:

- `type`: must be `file`
- `path`: output path. Relative paths are resolved from the YAML file directory.
- `format`: must be `csv` or `jsonl`
- `mode`: optional, defaults to `overwrite`; supported values are `overwrite`
  and `append`

CSV output rules:

- a header row is written before data rows when the file is created or
  overwritten,
- `append` writes the header only when the target file does not exist or is
  empty,
- `LONG`, `STRING`, and `VECTOR` output fields are supported,
- string fields and header fields containing commas, quotes, newlines, or
  carriage returns are written as quoted CSV fields, with quotes escaped by
  doubling them,
- vector fields are written as bracketed, space-delimited values such as
  `[5.0000 6.0000 7.0000 8.0000]`.

JSONL output rules:

- one JSON object is written per output row,
- no header row is written,
- `LONG`, `STRING`, and `VECTOR` output fields are supported,
- string field names and values are JSON-escaped,
- vector fields are written as JSON arrays.

### mock-vector

```yaml
sink:
  type: mock-vector
```

Prints vector summary output:

```text
[AI Sink] Row ID: 1, Vector Dim: 4, First Val: 5.0000
```

Rules:

- output row type must include `id` as `LONG`,
- output row type must include `embedding` as `VECTOR`.

Implementation note: `mock-vector` is backed by Kuaia's local mock vector sink
factory. The sink factory registry is an internal extension point for vector
database integrations. Use `qdrant`, `pgvector`, or `milvus` for current real
vector database sinks.

### qdrant

```yaml
sink:
  type: qdrant
  url: http://localhost:6333
  collection: kuaia_docs
  idField: id
  vectorField: embedding
  chunkIndexField: chunk_index
  chunkIdMultiplier: 1000000
  payloadFields: [id, chunk, chunk_index, chunk_start, chunk_end]
  wait: true
  timeoutMs: 30000
```

`sink.type: qdrant` writes vectors to Qdrant with
`PUT /collections/{collection}/points`. Local vector pipelines send rows in
batches when an upstream embedding transform has `batchSize` configured.

Fields:

- `type`: must be `qdrant`
- `url`: Qdrant HTTP base URL, for example `http://localhost:6333`
- `collection`: target collection name
- `idField`: `LONG` field used as the Qdrant point id
- `vectorField`: `VECTOR` field used as the Qdrant vector
- `payloadFields`: optional list of `LONG` or `STRING` fields to include in the
  Qdrant payload; when omitted, all non-vector fields are included
- `chunkIndexField`: optional `LONG` field used to generate unique point ids
  for chunked rows
- `chunkIdMultiplier`: optional positive multiplier used with
  `chunkIndexField`; defaults to `1000000`
- `apiKeyEnv`: optional environment variable containing the Qdrant API key
- `wait`: optional, defaults to `true`; controls Qdrant's `wait` query parameter
- `timeoutMs`: optional HTTP connect/read timeout in milliseconds. Defaults to
  `30000` and must be a positive integer when configured.

When `payloadFields` is set, Kuaia sends only those fields as Qdrant payload.
The vector field cannot be included. This keeps large source text or temporary
transform fields out of vector payloads while preserving the metadata needed for
retrieval.

When `chunkIndexField` is set, the Qdrant point id is generated as
`idField * chunkIdMultiplier + chunkIndexField`. The chunked example combines
that with `payloadFields` to keep the original document id, chunk text, chunk
index, and character offsets while each chunk gets a stable point id. This is
intended for pipelines that run `chunk` before writing to Qdrant.

Qdrant collections are not created automatically. Create the example collection
before running `examples/local-file-to-qdrant.yaml`:

```bash
curl -X PUT http://localhost:6333/collections/kuaia_docs \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
```

When `apiKeyEnv` is configured, Kuaia reads that environment variable at runtime
and sends it as Qdrant's `api-key` header. Missing API keys, missing id/vector
fields, non-2xx Qdrant responses, and 2xx responses whose JSON `status` is not
`ok` are fatal sink errors.

### pgvector

```yaml
sink:
  type: pgvector
  url: jdbc:postgresql://localhost:5432/kuaia
  table: document_vectors
  userEnv: KUAIA_POSTGRES_USER
  passwordEnv: KUAIA_POSTGRES_PASSWORD
  idField: id
  vectorField: embedding
  payloadFields: [content]
  timeoutMs: 30000
```

`sink.type: pgvector` writes vectors to a pre-created PostgreSQL table that has
a pgvector `vector` column. Kuaia writes rows with JDBC batch upserts using
`INSERT ... ON CONFLICT (id) DO UPDATE`.

Fields:

- `type`: must be `pgvector`
- `url`: PostgreSQL JDBC URL
- `table`: target table name, optionally schema-qualified
- `userEnv`: environment variable containing the database user
- `passwordEnv`: environment variable containing the database password
- `idField`: `LONG` field used as the table primary key column
- `vectorField`: `VECTOR` field written to the pgvector column
- `payloadFields`: optional list of `LONG` or `STRING` fields to write as
  additional table columns; when omitted, all non-id and non-vector fields are
  included
- `timeoutMs`: optional JDBC connect/socket timeout in milliseconds. Defaults
  to `30000` and must be a positive integer when configured.

Kuaia does not create pgvector tables automatically. Create the target table
before running `examples/postgres-to-pgvector.yaml`:

```sql
create extension if not exists vector;

create table if not exists document_vectors (
  id bigint primary key,
  content text not null,
  embedding vector(4) not null
);
```

The id field and vector field cannot be included in `payloadFields`, since they
are already written as dedicated columns. Missing credential environment
variables, missing id/vector/payload fields, unsupported payload field types, and
JDBC write failures are fatal sink errors.

### milvus

```yaml
sink:
  type: milvus
  url: http://localhost:19530
  collection: kuaia_docs
  apiKeyEnv: KUAIA_MILVUS_TOKEN
  idField: id
  vectorField: embedding
  payloadFields: [content]
  timeoutMs: 30000
```

`sink.type: milvus` writes vectors to a pre-created Milvus collection using the
Milvus REST v2 `POST /v2/vectordb/entities/upsert` endpoint. Kuaia sends rows in
batches when an upstream embedding transform has `batchSize` configured.

Fields:

- `type`: must be `milvus`
- `url`: Milvus REST base URL, for example `http://localhost:19530`
- `collection`: target collection name
- `apiKeyEnv`: optional environment variable containing the Milvus bearer token,
  such as `root:Milvus` for a local authenticated standalone deployment
- `idField`: `LONG` field used as the collection primary key field
- `vectorField`: `VECTOR` field used as the Milvus vector field
- `payloadFields`: optional list of `LONG` or `STRING` fields to include as
  scalar or dynamic fields; when omitted, all non-id and non-vector fields are
  included
- `timeoutMs`: optional HTTP connect/read timeout in milliseconds. Defaults to
  `30000` and must be a positive integer when configured.

Kuaia does not create Milvus collections automatically. Create the example
collection before running `examples/local-file-to-milvus.yaml`:

```bash
export KUAIA_MILVUS_TOKEN=root:Milvus

curl -X POST http://localhost:19530/v2/vectordb/collections/create \
  -H "Authorization: Bearer $KUAIA_MILVUS_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{
    "collectionName": "kuaia_docs",
    "schema": {
      "autoID": false,
      "enableDynamicField": true,
      "fields": [
        {"fieldName": "id", "dataType": "Int64", "isPrimary": true, "autoID": false},
        {"fieldName": "embedding", "dataType": "FloatVector", "elementTypeParams": {"dim": 4}}
      ]
    }
  }'
```

The id field and vector field cannot be included in `payloadFields`, since they
are always sent as dedicated entity fields. Missing token environment variables,
missing id/vector/payload fields, unsupported payload field types, non-2xx Milvus
responses, and 2xx responses whose JSON `code` is not `0` are fatal sink errors.

## Error Policy

```yaml
errorPolicy:
  mode: fail-fast
```

`errorPolicy` controls how local pipelines handle malformed source rows.

Fields:

- `mode`: optional when `errorPolicy` is present, defaults to `fail-fast`

Supported modes:

- `fail-fast`: default behavior. The first malformed source row fails the run.
- `skip-bad-records`: malformed source rows are counted, reported, and skipped.

When `skip-bad-records` is enabled, Kuaia prints each skipped row:

```text
Skipped bad record seq=2 error=Invalid CSV row at line 3: expected 2 columns but found 1
```

For checkpointed local runs, skipped bad records count as consumed and advance
the checkpoint sequence. Pipeline definition errors, transform schema errors,
and sink IO errors remain fatal.

## Connector Boundary

The public YAML contract remains source -> transforms -> sink. Internally, Kuaia
adapts current sources into a connector API v2 shape with split-aware source
readers and batch-aware sink writers. File sources enumerate bounded
`SourceSplit` ranges internally, while other current sources use a single split.
This is an implementation boundary for future connector work; it does not add
CDC, parallel split scheduling, DAGs, or exactly-once guarantees to the current
MVP contract.

## Checkpoint

```yaml
checkpoint:
  stateDir: .kuaia/state/local-file-to-vector
```

When `checkpoint.stateDir` is set, Kuaia persists local task progress with
RocksDB. File data rows use 1-based source `seqId` values. For non-batched
pipelines, a checkpoint advances after a transformed row is successfully written
to the sink. For batch-aware vector pipelines, a successful sink batch advances
the checkpoint once to the highest source `seqId` in that batch. If the batch
write fails, the checkpoint does not advance and a rerun may replay that batch.
With `errorPolicy.mode: skip-bad-records`, a skipped malformed source row also
advances the checkpoint so reruns do not repeatedly process the same bad row.

On rerun, Kuaia skips source rows at or before the last checkpoint. If the task is
already `COMPLETED`, rerunning the same YAML prints `rows=0` and does not emit
completed rows again.

The MVP guarantee is at-least-once style execution with idempotent sinks. Kuaia
does not claim exactly-once semantics.

## Local Path Guardrails

By default, Kuaia preserves the local CLI path behavior:

- `source.path` and file `sink.path` are resolved from the YAML file directory,
- `checkpoint.stateDir` is passed through as configured.

For stricter local runs, set:

```bash
export KUAIA_RESTRICT_LOCAL_PATHS=true
```

When enabled, Kuaia rejects local file paths that resolve outside the YAML file
directory or the repository `.kuaia/` runtime directory. This applies to
`source.path`, file `sink.path`, and `checkpoint.stateDir`. Relative
`checkpoint.stateDir` values under `.kuaia/...` are anchored at the repository
`.kuaia/` directory in restricted mode.

## Run Summary

Successful declarative runs print a stable summary line:

```text
Run Summary: rowsRead=2 rowsWritten=2 rowsFailed=0 rowsSkipped=0 checkpointSeq=2 taskState=COMPLETED sourceSplits=1 sinkBatches=2 durationMs=12
```

Use `--summary-json <path>` when scripts need a machine-readable summary:

```bash
bin/kuaia run -f examples/local-file-to-file.yaml --summary-json .kuaia/output/local-file-to-file-summary.json
```

The JSON file uses stable field names:

```json
{"pipelineName":"local-file-to-file","rowsRead":2,"rowsWritten":2,"rowsFailed":0,"rowsSkipped":0,"checkpointSeq":2,"taskState":"COMPLETED","sourceSplits":1,"sinkBatches":2,"durationMs":12}
```

Fields:

- `pipelineName`: pipeline name from the YAML config, present only in JSON
  summary output,
- `rowsRead`: source rows read after checkpoint skips,
- `rowsWritten`: output rows successfully written to the sink,
- `rowsFailed`: malformed source rows skipped under `skip-bad-records`,
- `rowsSkipped`: rows skipped because the checkpoint already covered them,
- `checkpointSeq`: latest source sequence reached by this run or prior
  checkpoint,
- `taskState`: final task state for the local pipeline run,
- `sourceSplits`: source split readers executed by the local runner,
- `sinkBatches`: sink batches successfully committed by the local runner,
- `durationMs`: wall-clock runtime in milliseconds.

## Failure Messages

Failed declarative runs include a stable stage prefix before the underlying
error:

```text
Source stage failed: Invalid CSV row at line 3: expected 2 columns but found 1
Transform stage failed: Unknown transform field: missing
Sink stage failed: Mock vector sink requires VECTOR field: embedding
Checkpoint stage failed: Pipeline task local-pipeline-example is FAILED
```

Stage prefixes identify whether the failure happened while reading source data,
building or applying transforms, creating and writing the sink, or updating
checkpoint state. Failed runs do not print `Run Summary:` or write
`--summary-json` output.

## Examples

For the full list of public examples, expected output, Docker quickstart, and
cleanup notes, see [`docs/examples.md`](examples.md).

Run local CSV to console:

```bash
bin/kuaia run -f examples/local-file-to-console.yaml
```

Run CSV through `select` and `rename`:

```bash
bin/kuaia run -f examples/local-file-transform-to-console.yaml
```

Run CSV to a local output file:

```bash
bin/kuaia run -f examples/local-file-to-file.yaml
cat .kuaia/output/local-file-to-file.csv
```

Run JSONL to a local JSONL output file:

```bash
bin/kuaia run -f examples/local-jsonl-to-file.yaml
cat .kuaia/output/local-jsonl-to-file.jsonl
```

Run CSV while skipping malformed rows:

```bash
bin/kuaia run -f examples/local-file-skip-bad-records.yaml
```

Run CSV through mock embedding to mock vector sink:

```bash
bin/kuaia run -f examples/local-file-to-vector.yaml
```

Run JSONL through mock embedding to mock vector sink:

```bash
bin/kuaia run -f examples/local-jsonl-to-vector.yaml
```

Run JSONL through `chunk` and mock embedding:

```bash
bin/kuaia run -f examples/local-jsonl-chunk-to-vector.yaml
```

Run FAQ JSONL through cleanup and mock embedding:

```bash
bin/kuaia run -f examples/local-faq-jsonl-to-vector.yaml
```

Run CSV through an OpenAI-compatible embedding provider:

```bash
export OPENAI_API_KEY=...
bin/kuaia run -f examples/local-file-to-openai-compatible-vector.yaml
```

Run batch Postgres through mock embedding into Qdrant:

```bash
docker compose -f docker-compose.postgres.yml -f docker-compose.qdrant.yml up -d
export KUAIA_POSTGRES_USER=kuaia
export KUAIA_POSTGRES_PASSWORD=kuaia
bin/kuaia run -f examples/postgres-to-qdrant.yaml
```

Run batch Postgres through mock embedding into pgvector:

```bash
docker compose -f docker-compose.postgres.yml up -d
export KUAIA_POSTGRES_USER=kuaia
export KUAIA_POSTGRES_PASSWORD=kuaia
bin/kuaia run -f examples/postgres-to-pgvector.yaml
```

Run local DuckDB SQL over CSV through mock embedding into Qdrant:

```bash
docker compose -f docker-compose.qdrant.yml up -d
bin/kuaia run -f examples/duckdb-csv-to-qdrant.yaml
```

Run a local document directory through mock embedding into Qdrant:

```bash
docker compose -f docker-compose.qdrant.yml up -d
bin/kuaia run -f examples/documents-to-qdrant.yaml
```

Run batch MySQL through mock embedding into Qdrant:

```bash
docker compose -f docker-compose.mysql.yml -f docker-compose.qdrant.yml up -d
export KUAIA_MYSQL_USER=kuaia
export KUAIA_MYSQL_PASSWORD=kuaia
bin/kuaia run -f examples/mysql-to-qdrant.yaml
```

## Validate Before Running

Use `validate` to check a pipeline without executing it:

```bash
bin/kuaia validate -f examples/local-file-to-file.yaml
```

For a local `source.type: file`, including `format: document`, validation opens
the source enough to infer the row type, builds the transform chain, and checks
sink field compatibility. It does not write sink output or checkpoint state.

For a remote `source.type: file` (an `s3://` path), for `source.type: duckdb`,
`source.type: postgres`, and `source.type: mysql`, validation parses the YAML
and connector options without connecting to the source. Because the row type is
not read up front, transform and sink row-type checks are deferred until `run`.

## Benchmark Smoke

Kuaia's benchmark smoke is a developer check for the local batch path. It
generates CSV input, prints a compact counter summary, and writes JSON output to
`target/kuaia-benchmark/local-pipeline-batch.json`. The JSON includes row,
embedding, checkpoint, source split, and sink batch counters. By default the
benchmark runs batch sizes `1`, `8`, `32`, and `128`.

```bash
bin/kuaia benchmark
```

For larger local runs, override the row count:

```bash
bin/kuaia benchmark --rows 10000
```

To stress source split behavior, add `--max-rows-per-split <rows>`. To choose a
different output path, add `--output <path>`. To compare specific batch sizes,
add a comma-separated list:

```bash
bin/kuaia benchmark --batch-sizes 16,64,256
```

To write CSV instead of JSON, add `--format csv`:

```bash
bin/kuaia benchmark --format csv --output target/kuaia-benchmark/local-pipeline-batch.csv
```

## Error Messages

Expected user errors return exit code `1` and print a deterministic message.
Common examples:

- `Pipeline config not found: <path>`
- `Missing required field: <field>`
- `Unsupported source.type: <value>`
- `Unsupported source.format: <value>`
- `Unsupported sink.type: <value>`
- `Unsupported sink.format: <value>`
- `Unsupported sink.mode: <value>`
- `Unsupported errorPolicy.mode: <value>`
- `Unsupported transform.type: <value>`
- `Unsupported transforms[0].provider: <value>`
- `source.path is only supported for local source types`
- `source.format is only supported for source.type: file`
- `source.maxRowsPerSplit is only supported for file formats csv and jsonl`
- `Unsupported source.documentType: <value>`
- `source.documentType is only supported for source.format: document`
- `source.type document-directory has been replaced by source.type: file with
  format: document`
- `source.type s3 has been replaced by source.type: file with an s3:// path`
- `source.path storage scheme hdfs:// is not yet supported`
- `source.path storage scheme <scheme>:// is not supported`
- `source.bucket is only supported for s3:// paths`
- `source.prefix is only supported for s3:// paths`
- `source.endpoint is only supported for s3:// paths`
- `source.region is only supported for s3:// paths`
- `source.accessKeyEnv is only supported for s3:// paths`
- `source.secretKeyEnv is only supported for s3:// paths`
- `source.pathStyleAccess is only supported for s3:// paths`
- `source.bucket is not supported for source.type: file; put the bucket in the
  s3:// path`
- `source.prefix is not supported for source.type: file; put the prefix in the
  s3:// path`
- `Missing S3 access key environment variable: <name>`
- `Missing S3 secret key environment variable: <name>`
- `S3 source list failed: <message>`
- `S3 source read failed at <key>: <message>`
- `source.url is only supported for JDBC source types`
- `source.userEnv is only supported for JDBC source types`
- `source.passwordEnv is only supported for JDBC source types`
- `source.userEnv is not supported for source.type: duckdb`
- `source.passwordEnv is not supported for source.type: duckdb`
- `source.query is only supported for JDBC source types`
- `source.fetchSize is only supported for JDBC source types`
- `source.url for source.type postgres must start with jdbc:postgresql:`
- `source.url for source.type mysql must start with jdbc:mysql:`
- `source.url for source.type duckdb must start with jdbc:duckdb:`
- `Invalid source.fetchSize: <value>`
- `sink.timeoutMs is only supported for sink.type: qdrant, pgvector, or milvus`
- `sink.payloadFields is only supported for sink.type: qdrant, pgvector, or
  milvus`
- `sink.payloadFields must not include sink.vectorField: <field>`
- `Duplicate value in sink.payloadFields: <field>`
- `Invalid sink.timeoutMs: <value>`
- `Invalid transform.dimensions: <value>`
- `Invalid transform.timeoutMs: <value>`
- `Invalid transform.batchSize: <value>`
- `Invalid transform.minLength: <value>`
- `Invalid transform.chunkSize: <value>`
- `Invalid transform.overlap: <value>`
- `Invalid transform.dropInput: <value>`
- `Invalid transform.includeOffsets: <value>`
- `transform.overlap must be smaller than transform.chunkSize`
- `Unsupported transforms[<n>].op: <value>`
- `Local path escapes allowed directories: <field>`
- `Missing API key environment variable: <name>`
- `Missing Postgres environment variable: <name>`
- `Postgres source query failed: <message>`
- `Postgres source read failed: <message>`
- `Invalid Postgres row seq=<seq>: field <field> is null`
- `Missing MySQL environment variable: <name>`
- `MySQL source query failed: <message>`
- `MySQL source read failed: <message>`
- `Invalid MySQL row seq=<seq>: field <field> is null`
- `DuckDB source query failed: <message>`
- `DuckDB source read failed: <message>`
- `Invalid DuckDB row seq=<seq>: field <field> is null`
- `Document path not found: <path>`
- `Document file is not a supported document: <path>` — with a
  ` (documentType: <type>)` suffix when `documentType` is not `auto`
- `Document source path is not a directory: <path>`
- `Document directory scan failed: <path>: <message>`
- `Document directory has no supported documents: <path>` — with a
  ` (documentType: <type>)` suffix when `documentType` is not `auto`
- `Document source read failed at <path>: <message>`
- `Embedding request failed with status <code>: <response>`
- `Embedding response did not contain an embedding vector`
- `Embedding response returned <n> embeddings but expected <m>`
- `Embedding response contained duplicate embedding index: <index>`
- `Embedding response index out of range: <index>`
- `Invalid embedding value: <value>`
- `Unknown transform field: <field>`
- `Duplicate transform field: <field>`
- `Transform field must be STRING: <field>`
- `Mock vector sink requires VECTOR field: embedding`
- `Qdrant sink requires LONG field: <field>`
- `Qdrant sink requires VECTOR field: <field>`
- `Qdrant sink requires payload field: <field>`
- `Qdrant payload field must not be the vector field: <field>`
- `Duplicate Qdrant payload field: <field>`
- `Missing Qdrant API key environment variable: <name>`
- `Qdrant upsert failed with status <code>: <response>`
- `Qdrant upsert failed: <message>`
- `File sink does not support field type: <type>`
- `Invalid CSV row at line <line>: expected <n> columns but found <m>`
- `Invalid JSONL row at line <line>: malformed JSON`
- `Invalid JSONL row at line <line>: field <field> must be a scalar value`
- `Invalid JSONL row at line <line>: unexpected field <field>`
- `Invalid JSONL row at line <line>: missing field <field>`

## Non-Goals

The current YAML contract does not support:

- generic YAML features beyond the documented shape,
- nested JSONL objects, arrays, or null values,
- token-based or semantic text chunking,
- transform DAGs,
- expression filters, joins, casts, or aggregations,
- CDC offsets,
- CDC or streaming external connectors,
- additional production-certified external connectors beyond batch PostgreSQL
  and MySQL,
- additional real vector databases beyond Qdrant, pgvector, and Milvus,
- provider-specific SDK integrations,
- production deployment settings,
- exactly-once guarantees.
