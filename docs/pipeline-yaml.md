# Pipeline YAML

Kuaia's current public pipeline YAML is a small MVP contract for local examples.
It is intentionally not a general YAML dialect or production job spec.

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
```

Fields:

- `type`: must be `file`
- `path`: local CSV path. Relative paths are resolved from the YAML file directory.
- `format`: must be `csv`

CSV rules:

- first line is the header,
- empty lines are skipped,
- every data row must have the same number of columns as the header,
- a field named `id` is parsed as `LONG`,
- other fields are parsed as `STRING`,
- quoted CSV fields are not supported in this MVP.

### postgres

```yaml
source:
  type: postgres
  url: jdbc:postgresql://localhost:5432/kuaia
  userEnv: KUAIA_POSTGRES_USER
  passwordEnv: KUAIA_POSTGRES_PASSWORD
  query: select id, content from documents order by id
```

`source.type: postgres` runs one batch JDBC query and streams the result rows
through the local pipeline. It is not a CDC source.

Fields:

- `type`: must be `postgres`
- `url`: PostgreSQL JDBC URL
- `userEnv`: environment variable containing the database user
- `passwordEnv`: environment variable containing the database password
- `query`: SQL query to execute

Postgres type rules:

- integer-like columns are exposed as `LONG`,
- all other columns are exposed as `STRING`,
- result rows use 1-based source sequence ids in query order,
- checkpoint reruns skip result rows at or before the last checkpoint sequence.

Credentials are read from the environment at runtime and are never stored in
YAML. Query failures and missing credential environment variables are fatal
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

### mock-embedding

```yaml
transforms:
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
```

`mock-embedding` appends a deterministic `VECTOR` field. It is a local mock for
AI-ready flow testing and does not call an external model.

Rules:

- `input` must exist and be `STRING`,
- `output` must not already exist,
- `dimensions` is optional and defaults to `4`,
- `dimensions` must be a positive integer.

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

Additional `openai-compatible` fields:

- `model`: required model id, for example `text-embedding-3-small`.
- `apiKeyEnv`: required environment variable name that contains the bearer token.
- `baseUrl`: optional, defaults to `https://api.openai.com/v1`. Set it to the
  `/v1` base URL of another OpenAI-compatible service when needed.

Kuaia sends one embedding request per input row. The request body contains
`input`, `model`, and `encoding_format: float`; when `dimensions` is configured,
it is included as a positive integer. The API key is read at runtime from
`apiKeyEnv` and is never stored in YAML.

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

Writes output rows as a local CSV file.

Fields:

- `type`: must be `file`
- `path`: output path. Relative paths are resolved from the YAML file directory.
- `format`: must be `csv`
- `mode`: optional, defaults to `overwrite`; supported values are `overwrite`
  and `append`

CSV output rules:

- a header row is written before data rows when the file is created or
  overwritten,
- `append` writes the header only when the target file does not exist or is
  empty,
- `LONG`, `STRING`, and `VECTOR` output fields are supported,
- string fields containing commas or newlines are rejected because quoted CSV
  fields are not part of this MVP contract,
- vector fields are written as bracketed, space-delimited values such as
  `[5.0000 6.0000 7.0000 8.0000]`.

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
factory. The sink factory registry is an internal extension point for future
vector database integrations. Use `qdrant` for the current real vector database
sink.

### qdrant

```yaml
sink:
  type: qdrant
  url: http://localhost:6333
  collection: kuaia_docs
  idField: id
  vectorField: embedding
  wait: true
```

`sink.type: qdrant` writes vectors to Qdrant with
`PUT /collections/{collection}/points`.

Fields:

- `type`: must be `qdrant`
- `url`: Qdrant HTTP base URL, for example `http://localhost:6333`
- `collection`: target collection name
- `idField`: `LONG` field used as the Qdrant point id
- `vectorField`: `VECTOR` field used as the Qdrant vector
- `apiKeyEnv`: optional environment variable containing the Qdrant API key
- `wait`: optional, defaults to `true`; controls Qdrant's `wait` query parameter

Qdrant collections are not created automatically. Create the example collection
before running `examples/local-file-to-qdrant.yaml`:

```bash
curl -X PUT http://localhost:6333/collections/kuaia_docs \
  -H 'Content-Type: application/json' \
  --data '{"vectors":{"size":4,"distance":"Cosine"}}'
```

When `apiKeyEnv` is configured, Kuaia reads that environment variable at runtime
and sends it as Qdrant's `api-key` header. Missing API keys, missing id/vector
fields, and non-2xx Qdrant responses are fatal sink errors.

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

## Checkpoint

```yaml
checkpoint:
  stateDir: .kuaia/state/local-file-to-vector
```

When `checkpoint.stateDir` is set, Kuaia persists local task progress with
RocksDB. CSV data rows use 1-based source `seqId` values. A checkpoint advances
after a transformed row is successfully written to the sink. With
`errorPolicy.mode: skip-bad-records`, a skipped malformed source row also
advances the checkpoint so reruns do not repeatedly process the same bad row.

On rerun, Kuaia skips source rows at or before the last checkpoint. If the task is
already `COMPLETED`, rerunning the same YAML prints `rows=0` and does not emit
completed rows again.

The MVP guarantee is at-least-once style execution with idempotent sinks. Kuaia
does not claim exactly-once semantics.

## Run Summary

Successful declarative runs print a stable summary line:

```text
Run Summary: rowsRead=2 rowsWritten=2 rowsFailed=0 rowsSkipped=0 checkpointSeq=2 taskState=COMPLETED durationMs=12
```

Fields:

- `rowsRead`: source rows read after checkpoint skips,
- `rowsWritten`: output rows successfully written to the sink,
- `rowsFailed`: malformed source rows skipped under `skip-bad-records`,
- `rowsSkipped`: rows skipped because the checkpoint already covered them,
- `checkpointSeq`: latest source sequence reached by this run or prior
  checkpoint,
- `taskState`: final task state for the local pipeline run,
- `durationMs`: wall-clock runtime in milliseconds.

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

Run CSV while skipping malformed rows:

```bash
bin/kuaia run -f examples/local-file-skip-bad-records.yaml
```

Run CSV through mock embedding to mock vector sink:

```bash
bin/kuaia run -f examples/local-file-to-vector.yaml
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
- `Invalid transform.dimensions: <value>`
- `Missing API key environment variable: <name>`
- `Missing Postgres environment variable: <name>`
- `Postgres source query failed: <message>`
- `Invalid Postgres row seq=<seq>: field <field> is null`
- `Embedding request failed with status <code>: <response>`
- `Embedding response did not contain an embedding vector`
- `Unknown transform field: <field>`
- `Duplicate transform field: <field>`
- `Transform field must be STRING: <field>`
- `Mock vector sink requires VECTOR field: embedding`
- `Qdrant sink requires LONG field: <field>`
- `Qdrant sink requires VECTOR field: <field>`
- `Missing Qdrant API key environment variable: <name>`
- `Qdrant upsert failed with status <code>: <response>`
- `File sink does not support quoted CSV fields`
- `File sink does not support field type: <type>`
- `Invalid CSV row at line <line>: expected <n> columns but found <m>`

## Non-Goals

The current YAML contract does not support:

- generic YAML features beyond the documented shape,
- quoted CSV parsing,
- transform DAGs,
- filters, joins, casts, or aggregations,
- CDC offsets,
- CDC or streaming external connectors,
- additional production-certified external connectors beyond batch PostgreSQL,
- additional real vector databases beyond Qdrant,
- provider-specific SDK integrations,
- production deployment settings,
- exactly-once guarantees.
