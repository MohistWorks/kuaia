# Release Checklist

This checklist is for preparing an open-source MVP release candidate. It does
not create a tag by itself.

## Public Product Boundary

- [ ] `README.md` describes Kuaia as an experimental AI-ready data integration
  runtime, not a full production DataOps platform.
- [ ] `docs/product-scope.md` explains the current MVP scope, target users, and
  non-goals.
- [ ] `docs/pipeline-yaml.md` documents the public YAML contract and its
  limitations.
- [ ] `docs/examples.md` lists the recommended no-service path and external
  service examples separately.
- [ ] `docs/connector-development.md` explains the current connector extension
  boundary without promising a plugin marketplace.

## Required Repository Files

- [ ] `LICENSE` is present.
- [ ] `SECURITY.md` is present and tells users how to report vulnerabilities.
- [ ] `CHANGELOG.md` is present and reflects the public MVP boundary.
- [ ] `CONTRIBUTING.md` is present and includes the local validation gate.
- [ ] `.gitignore` excludes local build/state/internal directories.
- [ ] `.dockerignore` excludes local state and internal planning files from
  Docker builds.

## Repository Hygiene

- [ ] Local state, generated output, and internal planning notes are not
  committed.
- [ ] No API keys, bearer tokens, passwords, or private URLs are committed.

## Local Validation Gate

Run from the repository root:

```bash
make release-gate
```

The release-gate wrapper expands to:

```bash
mvn -q test
mvn -q package
bin/kuaia help
bin/kuaia examples
bin/kuaia validate -f examples/local-file-to-file.yaml
bin/kuaia validate -f examples/local-jsonl-chunk-to-qdrant.yaml
bin/kuaia validate -f examples/document-directory-to-qdrant.yaml
bin/kuaia validate -f examples/duckdb-csv-to-qdrant.yaml
bin/kuaia validate -f examples/s3-docs-to-qdrant.yaml
bin/kuaia validate -f examples/postgres-to-qdrant.yaml
bin/kuaia validate -f examples/mysql-to-qdrant.yaml
make public-mvp-smoke
make e2e
docker compose config
docker compose -f docker-compose.mysql.yml config
git diff --check
```

## CI Gate

- [ ] The protected `main` branch requires GitHub Actions `CI` to pass before
  pull requests can merge.
- [ ] GitHub Actions `CI` completes successfully on `main`.
- [ ] CI includes `make release-gate`.
- [ ] CI validates Maven tests, packaging, CLI help, local file preflight,
  Qdrant preflight, S3/Postgres/MySQL config preflight, public smoke,
  connector e2e, default Docker Compose config, and MySQL Docker Compose
  config.

## Release Decision

Before tagging or announcing an MVP release:

- [ ] Confirm whether the release should be tagless or tagged.
- [ ] Confirm the exact version name if tagging.
- [ ] Confirm the target repository and branch.
- [ ] Update `CHANGELOG.md` for the final public state.
- [ ] After tagging a release, bump `main` to the next development version.
- [ ] Re-run the local validation gate after the final commit.
- [ ] Confirm the latest remote CI run is successful.
