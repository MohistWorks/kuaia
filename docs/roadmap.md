# Roadmap

Kuaia is currently a local, checkpoint-aware AI-ready data pipeline runtime. The
`0.1.3` line focuses on making the local RAG data preparation path more
reliable and easier to validate before Kuaia expands into distributed execution,
CDC, or a larger connector catalog.

## 0.1.3 Status

0.1.3 is ready for release-candidate validation when the public validation gate
below passes on the final release branch.

Shipped in the `0.1.3-SNAPSHOT` line:

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

## Deferred Beyond 0.1.3

The following remain future work and should not block `0.1.3`:

- CDC or streaming sources,
- transform DAGs, joins, branches, or fan-out,
- exactly-once execution,
- Kubernetes or distributed HA operation,
- web UI, RBAC, lineage, or governance features,
- large connector catalog expansion,
- MySQL batch source,
- broad vector database support beyond Qdrant.

## Release Criteria

`0.1.3` is ready when the documented focus areas have either shipped or been
explicitly deferred, and the public validation gate passes:

```bash
mvn -q test
mvn -q package
bin/kuaia help
bin/kuaia examples
bin/kuaia validate -f examples/local-file-to-file.yaml
bin/kuaia validate -f examples/local-jsonl-chunk-to-qdrant.yaml
bin/kuaia validate -f examples/postgres-to-qdrant.yaml
make public-mvp-smoke
docker compose config
git diff --check
```
