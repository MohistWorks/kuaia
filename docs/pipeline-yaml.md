# Pipeline YAML

Kuaia's current public pipeline YAML is a small MVP contract for local examples.
It is intentionally not a general YAML dialect or production job spec.

## Shape

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
sink:
  type: mock-vector
checkpoint:
  stateDir: .kuaia/state/local-file-to-vector
```

Required top-level fields:

- `name`
- `source`
- `sink`

Optional top-level fields:

- `transforms`
- `checkpoint`

## Source

Supported source:

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

## Sink

### console

```yaml
sink:
  type: console
```

Prints all output row fields supported by the current console sink.

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

## Checkpoint

```yaml
checkpoint:
  stateDir: .kuaia/state/local-file-to-vector
```

When `checkpoint.stateDir` is set, Kuaia persists local task progress with
RocksDB. CSV data rows use 1-based source `seqId` values. A checkpoint advances
only after the transformed row is successfully written to the sink.

On rerun, Kuaia skips source rows at or before the last checkpoint. If the task is
already `COMPLETED`, rerunning the same YAML prints `rows=0` and does not emit
completed rows again.

The MVP guarantee is at-least-once style execution with idempotent sinks. Kuaia
does not claim exactly-once semantics.

## Examples

Run local CSV to console:

```bash
bin/kuaia run -f examples/local-file-to-console.yaml
```

Run CSV through `select` and `rename`:

```bash
bin/kuaia run -f examples/local-file-transform-to-console.yaml
```

Run CSV through mock embedding to mock vector sink:

```bash
bin/kuaia run -f examples/local-file-to-vector.yaml
```

## Error Messages

Expected user errors return exit code `1` and print a deterministic message.
Common examples:

- `Pipeline config not found: <path>`
- `Missing required field: <field>`
- `Unsupported source.type: <value>`
- `Unsupported source.format: <value>`
- `Unsupported sink.type: <value>`
- `Unsupported transform.type: <value>`
- `Invalid transform.dimensions: <value>`
- `Unknown transform field: <field>`
- `Duplicate transform field: <field>`
- `Transform field must be STRING: <field>`
- `Mock vector sink requires VECTOR field: embedding`
- `Invalid CSV row at line <line>: expected <n> columns but found <m>`

## Non-Goals

The current YAML contract does not support:

- generic YAML features beyond the documented shape,
- quoted CSV parsing,
- transform DAGs,
- filters, joins, casts, or aggregations,
- CDC offsets,
- real external connectors,
- real embedding providers,
- real vector databases,
- production deployment settings,
- exactly-once guarantees.
