# Product Scope

Kuaia is being built toward an AI-ready DataOps engine: one runtime for moving,
transforming, vectorizing, and activating data across local and distributed
deployments.

The current public repository is an MVP. Its job is to prove a smaller and more
concrete product slice: developers can run, inspect, and recover local AI-ready
data pipelines from a declarative YAML file.

## Current Product Definition

Kuaia today is a local Java runtime for checkpoint-aware batch pipelines:

```text
Source -> BinaryRow -> Transforms -> Sink
```

The MVP focuses on execution semantics rather than platform breadth. It should
be evaluated by whether a pipeline can be run locally, resumed after completed
work, extended through connector boundaries, and explained through deterministic
state transitions.

## Who It Is For Today

Kuaia is currently useful for:

- developers evaluating AI/RAG data preparation flows,
- contributors exploring source, transform, and sink connector boundaries,
- users who want a small local pipeline from CSV, JSONL, Postgres, or MySQL into
  CSV/JSONL files, console output, mock vector output, or Qdrant,
- maintainers validating checkpoint and state-store behavior before broader
  distributed execution work.

It is not yet aimed at production teams that need CDC, streaming joins, a visual
control plane, RBAC, lineage, Kubernetes operation, or a large connector catalog.

## What You Can Run Today

Supported sources:

- local CSV and JSONL files through `source.type: file`,
- batch PostgreSQL queries through `source.type: postgres`,
- batch MySQL queries through `source.type: mysql`.

Supported transforms:

- `select`,
- `rename`,
- `trim`,
- `lowercase`,
- `replace`,
- `filter` with `op: not-empty`, `op: min-length`, `op: contains`,
  `op: starts-with`, `op: ends-with`, `op: equals`, `op: not-equals`,
  `op: greater-than`, `op: greater-than-or-equal`, `op: less-than`, and
  `op: less-than-or-equal`,
- `chunk`,
- deterministic local `mock-embedding`,
- OpenAI-compatible `embedding` requests.

Supported sinks:

- `console`,
- local CSV or JSONL `file`,
- `mock-vector`,
- Qdrant vector upserts.

Supported runtime behavior:

- declarative YAML pipelines,
- preflight validation for local file pipelines,
- linear transform chains,
- RocksDB-backed local checkpoints,
- bad-record skipping for malformed CSV or JSONL rows,
- text trimming, lowercasing, literal replacement, empty-text filtering,
  minimum-length filtering, and case-sensitive substring, prefix, suffix, and
  exact-match filtering before embedding or chunking,
- numeric comparison filtering for `LONG` fields,
- character-based text chunking for local document pipelines,
- chunk payload controls for omitting repeated source text and keeping
  character offsets,
- batch-aware embedding and vector sink execution,
- Qdrant point id generation for chunked document pipelines,
- Qdrant payload field selection for predictable vector metadata,
- run summaries with row, checkpoint, split, and sink batch counters.

## Current Contract

The public contract is documented in [`pipeline-yaml.md`](pipeline-yaml.md).
The example catalog is documented in [`examples.md`](examples.md). Connector
extension notes are documented in
[`connector-development.md`](connector-development.md).

The current execution guarantee is at-least-once style processing with
idempotent sinks. Kuaia does not claim exactly-once execution in the MVP.

The current topology is linear. Kuaia does not yet expose DAGs, joins, branches,
fan-out, streaming windows, or CDC offset coordination.

## Roadmap Direction

The long-term direction remains broader than the MVP:

- richer connector APIs and production-certified connectors,
- more vector database and embedding provider integrations,
- stronger batching and performance baselines,
- distributed scheduling and high availability,
- governance, lineage, and operational control-plane capabilities.

Public docs should describe these as future direction until they are shipped in
the open-source runtime.
