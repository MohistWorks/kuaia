# Kuaia Documentation

This directory contains public, user-facing documentation for the current Kuaia
MVP. Internal design notes are not part of this tree.

## Start Here

- [`product-scope.md`](product-scope.md): what Kuaia is today, who it is for,
  and what is outside the MVP.
- [`roadmap.md`](roadmap.md): current release status, validation gate, and
  deferred work.
- [`examples.md`](examples.md): runnable examples, including the recommended
  no-service smoke path.
- [`pipeline-yaml.md`](pipeline-yaml.md): the current declarative pipeline YAML
  contract.

## Running Distributed

- [`distributed-quickstart.md`](distributed-quickstart.md): run a coordinator
  and a worker across processes, with restart recovery and runtime submission.
- [`ha-quickstart.md`](ha-quickstart.md): a highly available 3-node coordinator
  cluster — leader-gated dispatch, worker leader auto-discovery, leader-crash
  failover, and runtime `cluster add-node`/`remove-node`.

## Contributor References

- [`connector-development.md`](connector-development.md): current source,
  transform, and sink extension boundaries.
- [`release-checklist.md`](release-checklist.md): open-source MVP release
  candidate checks.
- [`../CHANGELOG.md`](../CHANGELOG.md): current public MVP release notes.

## Visual References

- [`visuals/binary-row-layout.html`](visuals/binary-row-layout.html): BinaryRow
  memory layout reference.
- [`visuals/coordinator-arch.html`](visuals/coordinator-arch.html): Coordinator
  architecture reference.
