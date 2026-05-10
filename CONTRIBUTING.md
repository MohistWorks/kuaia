# Contributing To Kuaia

Kuaia is currently an MVP runtime. Contributions should keep changes small,
testable, and aligned with the current public product scope in
[`docs/product-scope.md`](docs/product-scope.md).

## Development Setup

Requirements:

- JDK 8 or newer
- Maven 3.8+

Run the focused public MVP smoke before submitting user-facing changes:

```bash
make public-mvp-smoke
```

Run the full local gate before submitting runtime or connector changes:

```bash
mvn -q test
mvn -q package
make public-mvp-smoke
git diff --check
```

## Workflow

1. Open an issue or discussion for broad design changes.
2. Keep pull requests focused on one behavior or documentation improvement.
3. Add or update tests before changing runtime behavior.
4. Keep public user-facing documentation in `README.md` or `docs/`.
5. Do not add internal planning notes or local design files to the public
   repository.
6. Do not commit local state, generated output, or machine-specific files such
   as `.kuaia/`, `target/`, `.DS_Store`, or `dev/`.

## Current Scope

The MVP supports local declarative batch pipelines, typed `BinaryRow` records,
checkpointed execution, local CSV and batch Postgres sources, file/console/mock
vector/Qdrant sinks, and a bounded Raft HA prototype for state-store validation.

The project does not yet provide CDC, streaming DAG execution, production
deployment packaging, a web UI, a connector marketplace, or exactly-once
guarantees.

## Connector Changes

Connector development notes live in
[`docs/connector-development.md`](docs/connector-development.md). Connector
changes should include:

- focused unit tests for parsing, schema/type handling, and error messages,
- an example YAML file when the connector can run locally without hidden
  services,
- documentation updates in [`docs/pipeline-yaml.md`](docs/pipeline-yaml.md),
- public MVP smoke coverage when the change affects default examples.
