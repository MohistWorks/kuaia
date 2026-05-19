#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

cd "$ROOT_DIR"

mvn -q test
mvn -q package
bin/kuaia help
bin/kuaia examples
bin/kuaia validate -f examples/local-file-to-file.yaml
bin/kuaia validate -f examples/local-jsonl-chunk-to-qdrant.yaml
bin/kuaia validate -f examples/document-directory-to-qdrant.yaml
bin/kuaia validate -f examples/duckdb-csv-to-qdrant.yaml
bin/kuaia validate -f examples/s3-docs-to-qdrant.yaml
bin/kuaia validate -f examples/local-file-to-milvus.yaml
bin/kuaia validate -f examples/postgres-to-qdrant.yaml
bin/kuaia validate -f examples/postgres-to-pgvector.yaml
bin/kuaia validate -f examples/mysql-to-qdrant.yaml
make public-mvp-smoke
make e2e CASE=all
docker compose config >/dev/null
docker compose -f docker-compose.postgres.yml config >/dev/null
docker compose -f docker-compose.mysql.yml config >/dev/null
git diff --check

echo "Release gate passed."
