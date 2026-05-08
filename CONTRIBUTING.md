# Contributing To Kuaia

Kuaia is currently an MVP runtime. Contributions should keep changes small, testable, and aligned with the current execution model.

## Development Setup

Requirements:

- JDK 8 or newer
- Maven 3.8+

Run the full test suite before submitting changes:

```bash
mvn -q test
git diff --check
```

## Workflow

1. Open an issue or discussion for broad design changes.
2. Keep pull requests focused on one behavior or documentation improvement.
3. Add or update tests before changing runtime behavior.
4. Keep public user-facing documentation in `README.md` or `docs/`.
5. Do not add internal planning notes or local design files to the public repository.

## Current Scope

The MVP supports local demo pipelines, task attempt/checkpoint state, RocksDB persistence, coordinator recovery planning, and a bounded Raft HA prototype.

The project does not yet provide production deployment packaging, real external connectors, a web UI, or exactly-once guarantees.
