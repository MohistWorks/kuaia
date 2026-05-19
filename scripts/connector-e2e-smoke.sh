#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
KUAIA_CMD=${KUAIA:-"$ROOT_DIR/bin/kuaia"}
RUN_ID=$(date +%Y%m%d%H%M%S)-$$
COMPOSE_PROJECT=$(printf "kuaia-e2e-%s" "$RUN_ID" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_-' '-')
WORK_DIR="$ROOT_DIR/.kuaia/connector-e2e-smoke/$RUN_ID"
E2E_COMPOSE="$WORK_DIR/docker-compose.e2e.yml"
QDRANT_URL=""
MILVUS_URL=""
MILVUS_TOKEN="root:Milvus"

list_cases() {
  echo "Available e2e cases:"
  echo "  all"
  echo "  file-qdrant"
  echo "  document-directory-qdrant"
  echo "  duckdb-qdrant"
  echo "  s3-qdrant"
  echo "  file-milvus"
  echo "  postgres-qdrant"
  echo "  postgres-pgvector"
  echo "  mysql-qdrant"
}

usage() {
  echo "Usage: $0 [all|file-qdrant|document-directory-qdrant|duckdb-qdrant|s3-qdrant|file-milvus|postgres-qdrant|postgres-pgvector|mysql-qdrant|--list|--help]"
  echo
  list_cases
}

if [ "$#" -gt 1 ]; then
  usage >&2
  exit 2
fi

case "${1:-${CASE:-all}}" in
  --help|-h)
    usage
    exit 0
    ;;
  --list)
    list_cases
    exit 0
    ;;
  all|file-qdrant|document-directory-qdrant|duckdb-qdrant|s3-qdrant|file-milvus|postgres-qdrant|postgres-pgvector|mysql-qdrant)
    SELECTED_CASE=${1:-${CASE:-all}}
    ;;
  *)
    echo "Unknown e2e case: ${1:-${CASE:-all}}" >&2
    usage >&2
    exit 2
    ;;
esac

compose() {
  docker compose -p "$COMPOSE_PROJECT" -f "$E2E_COMPOSE" "$@"
}

cleanup() {
  status=$?
  if [ -f "$E2E_COMPOSE" ]; then
    compose down -v >/dev/null 2>&1 || true
  fi
  exit "$status"
}

case_selected() {
  [ "$SELECTED_CASE" = "all" ] || [ "$SELECTED_CASE" = "$1" ]
}

needs_postgres() {
  case_selected postgres-qdrant || case_selected postgres-pgvector
}

needs_qdrant() {
  case_selected file-qdrant \
    || case_selected document-directory-qdrant \
    || case_selected duckdb-qdrant \
    || case_selected s3-qdrant \
    || case_selected postgres-qdrant \
    || case_selected mysql-qdrant
}

needs_milvus() {
  case_selected file-milvus
}

needs_mysql() {
  case_selected mysql-qdrant
}

needs_minio() {
  case_selected s3-qdrant || needs_milvus
}

wait_for_qdrant() {
  attempts=0
  while [ "$attempts" -lt 60 ]; do
    if curl -fsS "$QDRANT_URL/collections" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done

  echo "Qdrant did not become ready." >&2
  compose logs qdrant >&2 || true
  return 1
}

wait_for_minio() {
  attempts=0
  while [ "$attempts" -lt 60 ]; do
    if curl -fsS "http://127.0.0.1:$MINIO_PORT/minio/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done

  echo "MinIO did not become ready." >&2
  compose logs minio >&2 || true
  return 1
}

wait_for_milvus() {
  attempts=0
  while [ "$attempts" -lt 90 ]; do
    if curl -fsS "http://127.0.0.1:$MILVUS_HEALTH_PORT/healthz" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done

  echo "Milvus did not become ready." >&2
  compose logs milvus >&2 || true
  return 1
}

setup_minio() {
  docker run --rm \
    --network "${COMPOSE_PROJECT}_default" \
    -v "$WORK_DIR/data/docs:/seed/docs:ro" \
    --entrypoint /bin/sh \
    minio/mc:latest \
    -c 'set -eu
attempts=0
until mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; do
  attempts=$((attempts + 1))
  if [ "$attempts" -ge 60 ]; then
    echo "Could not configure MinIO alias." >&2
    exit 1
  fi
  sleep 2
done
mc mb -p local/kuaia-docs >/dev/null 2>&1 || true
mc cp --recursive /seed/docs/ local/kuaia-docs/docs/ >/dev/null'
}

create_milvus_collection() {
  collection=$1
  response=$(curl -fsS \
    -X POST "$MILVUS_URL/v2/vectordb/collections/create" \
    -H "Authorization: Bearer $MILVUS_TOKEN" \
    -H "Content-Type: application/json" \
    --data "{\"collectionName\":\"$collection\",\"schema\":{\"autoID\":false,\"enableDynamicField\":true,\"fields\":[{\"fieldName\":\"id\",\"dataType\":\"Int64\",\"isPrimary\":true,\"autoID\":false},{\"fieldName\":\"embedding\",\"dataType\":\"FloatVector\",\"elementTypeParams\":{\"dim\":4}}]},\"indexParams\":[{\"fieldName\":\"embedding\",\"metricType\":\"COSINE\",\"indexType\":\"AUTOINDEX\"}]}" \
    | tr -d '[:space:]')

  case "$response" in
    *"\"code\":0"*) ;;
    *)
      echo "Could not create Milvus collection $collection." >&2
      echo "Actual response: $response" >&2
      return 1
      ;;
  esac
}

load_milvus_collection() {
  collection=$1
  response=$(curl -fsS \
    -X POST "$MILVUS_URL/v2/vectordb/collections/load" \
    -H "Authorization: Bearer $MILVUS_TOKEN" \
    -H "Content-Type: application/json" \
    --data "{\"collectionName\":\"$collection\"}" \
    | tr -d '[:space:]')

  case "$response" in
    *"\"code\":0"*) ;;
    *)
      echo "Could not load Milvus collection $collection." >&2
      echo "Actual response: $response" >&2
      return 1
      ;;
  esac

  attempts=0
  while [ "$attempts" -lt 60 ]; do
    response=$(curl -fsS \
      -X POST "$MILVUS_URL/v2/vectordb/collections/get_load_state" \
      -H "Authorization: Bearer $MILVUS_TOKEN" \
      -H "Content-Type: application/json" \
      --data "{\"collectionName\":\"$collection\"}" \
      | tr -d '[:space:]')

    case "$response" in
      *"\"loadState\":\"LoadStateLoaded\""*) return 0 ;;
      *) ;;
    esac

    attempts=$((attempts + 1))
    sleep 1
  done

  echo "Milvus collection $collection did not load." >&2
  echo "Actual response: $response" >&2
  return 1
}

wait_for_postgres() {
  attempts=0
  while [ "$attempts" -lt 60 ]; do
    if compose exec -T postgres pg_isready -U kuaia -d kuaia >/dev/null 2>&1 \
      && compose exec -T postgres psql -U kuaia -d kuaia -tAc "select count(*) from documents" | grep -q "2"; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done

  echo "Postgres did not become ready with example data." >&2
  compose logs postgres >&2 || true
  return 1
}

wait_for_mysql() {
  attempts=0
  while [ "$attempts" -lt 60 ]; do
    if compose exec -T mysql mysqladmin ping -h 127.0.0.1 -u kuaia -pkuaia --silent >/dev/null 2>&1 \
      && compose exec -T mysql mysql -h 127.0.0.1 -u kuaia -pkuaia --skip-column-names kuaia \
        -e "select count(*) from documents" | grep -q "3"; then
      return 0
    fi
    attempts=$((attempts + 1))
    sleep 2
  done

  echo "MySQL did not become ready with example data." >&2
  compose logs mysql >&2 || true
  return 1
}

create_qdrant_collection() {
  collection=$1
  curl -fsS \
    -X PUT "$QDRANT_URL/collections/$collection" \
    -H "Content-Type: application/json" \
    --data '{"vectors":{"size":4,"distance":"Cosine"}}' \
    >/dev/null
}

service_port() {
  service=$1
  port=$2
  host_port=$(compose port "$service" "$port" | sed -n 's/.*:\([0-9][0-9]*\)$/\1/p' | tail -n 1)
  if [ -z "$host_port" ]; then
    echo "Could not resolve mapped port for $service:$port." >&2
    return 1
  fi
  printf '%s\n' "$host_port"
}

require_contains() {
  file=$1
  expected=$2
  if ! grep -F "$expected" "$file" >/dev/null; then
    echo "Expected $file to contain: $expected" >&2
    echo "Actual content:" >&2
    cat "$file" >&2
    return 1
  fi
}

run_pipeline_with_summary() {
  name=$1
  config=$2
  summary=$3
  expected_read=$4
  expected_written=$5

  "$KUAIA_CMD" run -f "$config" --summary-json "$summary"
  require_contains "$summary" "\"pipelineName\":\"$name\""
  require_contains "$summary" "\"rowsRead\":$expected_read"
  require_contains "$summary" "\"rowsWritten\":$expected_written"
  require_contains "$summary" "\"rowsFailed\":0"
  require_contains "$summary" "\"taskState\":\"COMPLETED\""
}

require_qdrant_count() {
  collection=$1
  expected=$2
  response=$(curl -fsS \
    -X POST "$QDRANT_URL/collections/$collection/points/count" \
    -H "Content-Type: application/json" \
    --data '{"exact":true}' | tr -d '[:space:]')

  case "$response" in
    *"\"count\":$expected"*) ;;
    *)
      echo "Expected Qdrant collection $collection to contain $expected points." >&2
      echo "Actual response: $response" >&2
      return 1
      ;;
  esac
}

require_milvus_count() {
  collection=$1
  expected=$2
  attempts=0
  response=""
  while [ "$attempts" -lt 30 ]; do
    response=$(curl -fsS \
      -X POST "$MILVUS_URL/v2/vectordb/entities/query" \
      -H "Authorization: Bearer $MILVUS_TOKEN" \
      -H "Content-Type: application/json" \
      --data "{\"collectionName\":\"$collection\",\"filter\":\"id >= 0\",\"outputFields\":[\"count(*)\"]}" \
      | tr -d '[:space:]')

    case "$response" in
      *"\"code\":0"*\""count(*)\":$expected"*) return 0 ;;
      *) ;;
    esac

    attempts=$((attempts + 1))
    sleep 1
  done

  echo "Expected Milvus collection $collection to contain $expected entities." >&2
  echo "Actual response: $response" >&2
  return 1
}

require_postgres_count() {
  table=$1
  expected=$2
  actual=$(compose exec -T postgres psql -U kuaia -d kuaia -tAc "select count(*) from $table" | tr -d '[:space:]')
  if [ "$actual" != "$expected" ]; then
    echo "Expected Postgres table $table to contain $expected rows, got $actual." >&2
    return 1
  fi
}

write_compose_file() {
  cat > "$E2E_COMPOSE" <<EOF
services:
EOF

  if needs_postgres; then
    cat >> "$E2E_COMPOSE" <<EOF
  postgres:
    image: pgvector/pgvector:pg16
    environment:
      POSTGRES_DB: kuaia
      POSTGRES_USER: kuaia
      POSTGRES_PASSWORD: kuaia
    ports:
      - "127.0.0.1::5432"
    volumes:
      - "$ROOT_DIR/examples/postgres/init:/docker-entrypoint-initdb.d:ro"
      - postgres-data:/var/lib/postgresql/data

EOF
  fi

  if needs_mysql; then
    cat >> "$E2E_COMPOSE" <<EOF
  mysql:
    image: mysql:8.4
    environment:
      MYSQL_DATABASE: kuaia
      MYSQL_USER: kuaia
      MYSQL_PASSWORD: kuaia
      MYSQL_ROOT_PASSWORD: kuaia-root
    ports:
      - "127.0.0.1::3306"
    volumes:
      - "$ROOT_DIR/examples/mysql/init:/docker-entrypoint-initdb.d:ro"

EOF
  fi

  if needs_minio; then
    cat >> "$E2E_COMPOSE" <<EOF
  minio:
    image: minio/minio:RELEASE.2024-12-18T13-15-44Z
    command: minio server /minio_data --console-address ":9001"
    environment:
      MINIO_ACCESS_KEY: minioadmin
      MINIO_SECRET_KEY: minioadmin
    ports:
      - "127.0.0.1::9000"
    volumes:
      - minio-data:/minio_data

EOF
  fi

  if needs_milvus; then
    cat >> "$E2E_COMPOSE" <<EOF
  etcd:
    image: quay.io/coreos/etcd:v3.5.25
    environment:
      ETCD_AUTO_COMPACTION_MODE: revision
      ETCD_AUTO_COMPACTION_RETENTION: "1000"
      ETCD_QUOTA_BACKEND_BYTES: "4294967296"
      ETCD_SNAPSHOT_COUNT: "50000"
    command:
      - etcd
      - --advertise-client-urls=http://etcd:2379
      - --listen-client-urls=http://0.0.0.0:2379
      - --data-dir=/etcd
    volumes:
      - etcd-data:/etcd

  milvus:
    image: milvusdb/milvus:v2.6.15
    command: ["milvus", "run", "standalone"]
    security_opt:
      - seccomp:unconfined
    environment:
      ETCD_ENDPOINTS: etcd:2379
      MINIO_ADDRESS: minio:9000
      MINIO_ACCESS_KEY: minioadmin
      MINIO_SECRET_KEY: minioadmin
      MQ_TYPE: woodpecker
    ports:
      - "127.0.0.1::19530"
      - "127.0.0.1::9091"
    volumes:
      - milvus-data:/var/lib/milvus
    depends_on:
      - etcd
      - minio

EOF
  fi

  if needs_qdrant; then
    cat >> "$E2E_COMPOSE" <<EOF
  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "127.0.0.1::6333"
    volumes:
      - qdrant-data:/qdrant/storage

EOF
  fi

  cat >> "$E2E_COMPOSE" <<EOF
volumes:
EOF

  if needs_postgres; then
    cat >> "$E2E_COMPOSE" <<EOF
  postgres-data:
EOF
  fi

  if needs_minio; then
    cat >> "$E2E_COMPOSE" <<EOF
  minio-data:
EOF
  fi

  if needs_milvus; then
    cat >> "$E2E_COMPOSE" <<EOF
  etcd-data:
  milvus-data:
EOF
  fi

  if needs_qdrant; then
    cat >> "$E2E_COMPOSE" <<EOF
  qdrant-data:
EOF
  fi
}

trap cleanup EXIT INT TERM

mkdir -p "$WORK_DIR/data" "$WORK_DIR/state" "$WORK_DIR/summaries"
cp "$ROOT_DIR/examples/data/articles.jsonl" "$WORK_DIR/data/articles.jsonl"
cp "$ROOT_DIR/examples/data/documents.csv" "$WORK_DIR/data/documents.csv"
cp -R "$ROOT_DIR/examples/data/docs" "$WORK_DIR/data/docs"

FILE_TO_QDRANT="$WORK_DIR/local-jsonl-chunk-to-qdrant.yaml"
FILE_TO_MILVUS="$WORK_DIR/local-file-to-milvus.yaml"
DOCUMENT_DIRECTORY_TO_QDRANT="$WORK_DIR/document-directory-to-qdrant.yaml"
DUCKDB_TO_QDRANT="$WORK_DIR/duckdb-csv-to-qdrant.yaml"
S3_TO_QDRANT="$WORK_DIR/s3-docs-to-qdrant.yaml"
POSTGRES_TO_QDRANT="$WORK_DIR/postgres-to-qdrant.yaml"
POSTGRES_TO_PGVECTOR="$WORK_DIR/postgres-to-pgvector.yaml"
MYSQL_TO_QDRANT="$WORK_DIR/mysql-to-qdrant.yaml"

write_compose_file

echo "Starting connector e2e case $SELECTED_CASE with Compose project $COMPOSE_PROJECT..."
compose up -d

POSTGRES_PORT=""
MYSQL_PORT=""
MINIO_PORT=""
MILVUS_PORT=""
MILVUS_HEALTH_PORT=""
if needs_postgres; then
  POSTGRES_PORT=$(service_port postgres 5432)
fi
if needs_mysql; then
  MYSQL_PORT=$(service_port mysql 3306)
fi
if needs_minio; then
  MINIO_PORT=$(service_port minio 9000)
fi
if needs_milvus; then
  MILVUS_PORT=$(service_port milvus 19530)
  MILVUS_HEALTH_PORT=$(service_port milvus 9091)
  MILVUS_URL="http://127.0.0.1:$MILVUS_PORT"
fi
if needs_qdrant; then
  QDRANT_PORT=$(service_port qdrant 6333)
  QDRANT_URL="http://127.0.0.1:$QDRANT_PORT"
fi
MINIO_ENDPOINT="http://127.0.0.1:$MINIO_PORT"

cat > "$FILE_TO_QDRANT" <<EOF
name: connector-e2e-jsonl-chunk-to-qdrant
source:
  type: file
  path: data/articles.jsonl
  format: jsonl
transforms:
  - type: select
    fields: [id, content]
  - type: trim
    field: content
  - type: filter
    field: content
    op: not-empty
  - type: chunk
    input: content
    output: chunk
    chunkSize: 12
    overlap: 2
    dropInput: true
    includeOffsets: true
  - type: mock-embedding
    input: chunk
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: qdrant
  url: $QDRANT_URL
  collection: kuaia_e2e_article_chunks
  idField: id
  vectorField: embedding
  chunkIndexField: chunk_index
  chunkIdMultiplier: 1000000
  payloadFields: [id, chunk, chunk_index, chunk_start, chunk_end]
  wait: true
  timeoutMs: 30000
checkpoint:
  stateDir: $WORK_DIR/state/local-jsonl-chunk-to-qdrant
EOF

cat > "$FILE_TO_MILVUS" <<EOF
name: connector-e2e-file-to-milvus
source:
  type: file
  path: data/documents.csv
  format: csv
transforms:
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: milvus
  url: $MILVUS_URL
  collection: kuaia_e2e_milvus_docs
  apiKeyEnv: KUAIA_MILVUS_TOKEN
  idField: id
  vectorField: embedding
  payloadFields: [content]
  timeoutMs: 30000
checkpoint:
  stateDir: $WORK_DIR/state/file-to-milvus
EOF

cat > "$DOCUMENT_DIRECTORY_TO_QDRANT" <<EOF
name: connector-e2e-document-directory-to-qdrant
source:
  type: document-directory
  path: $WORK_DIR/data/docs
transforms:
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: qdrant
  url: $QDRANT_URL
  collection: kuaia_e2e_document_directory_docs
  idField: id
  vectorField: embedding
  payloadFields: [id, path, content]
  wait: true
  timeoutMs: 30000
checkpoint:
  stateDir: $WORK_DIR/state/document-directory-to-qdrant
EOF

cat > "$DUCKDB_TO_QDRANT" <<EOF
name: connector-e2e-duckdb-to-qdrant
source:
  type: duckdb
  query: select id, content from read_csv_auto('$WORK_DIR/data/documents.csv') order by id
transforms:
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: qdrant
  url: $QDRANT_URL
  collection: kuaia_e2e_duckdb_docs
  idField: id
  vectorField: embedding
  payloadFields: [id, content]
  wait: true
  timeoutMs: 30000
checkpoint:
  stateDir: $WORK_DIR/state/duckdb-to-qdrant
EOF

cat > "$S3_TO_QDRANT" <<EOF
name: connector-e2e-s3-to-qdrant
source:
  type: s3
  endpoint: $MINIO_ENDPOINT
  region: us-east-1
  bucket: kuaia-docs
  prefix: docs/
  accessKeyEnv: KUAIA_S3_ACCESS_KEY
  secretKeyEnv: KUAIA_S3_SECRET_KEY
  pathStyleAccess: true
transforms:
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: qdrant
  url: $QDRANT_URL
  collection: kuaia_e2e_s3_docs
  idField: id
  vectorField: embedding
  payloadFields: [id, key, content]
  wait: true
  timeoutMs: 30000
checkpoint:
  stateDir: $WORK_DIR/state/s3-to-qdrant
EOF

cat > "$POSTGRES_TO_QDRANT" <<EOF
name: connector-e2e-postgres-to-qdrant
source:
  type: postgres
  url: jdbc:postgresql://127.0.0.1:$POSTGRES_PORT/kuaia
  userEnv: KUAIA_POSTGRES_USER
  passwordEnv: KUAIA_POSTGRES_PASSWORD
  query: select id, content from documents order by id
  fetchSize: 1000
transforms:
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: qdrant
  url: $QDRANT_URL
  collection: kuaia_e2e_pg_docs
  idField: id
  vectorField: embedding
  payloadFields: [id, content]
  wait: true
  timeoutMs: 30000
checkpoint:
  stateDir: $WORK_DIR/state/postgres-to-qdrant
EOF

cat > "$POSTGRES_TO_PGVECTOR" <<EOF
name: connector-e2e-postgres-to-pgvector
source:
  type: postgres
  url: jdbc:postgresql://127.0.0.1:$POSTGRES_PORT/kuaia
  userEnv: KUAIA_POSTGRES_USER
  passwordEnv: KUAIA_POSTGRES_PASSWORD
  query: select id, content from documents order by id
  fetchSize: 1000
transforms:
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: pgvector
  url: jdbc:postgresql://127.0.0.1:$POSTGRES_PORT/kuaia
  table: document_vectors
  userEnv: KUAIA_POSTGRES_USER
  passwordEnv: KUAIA_POSTGRES_PASSWORD
  idField: id
  vectorField: embedding
  payloadFields: [content]
  timeoutMs: 30000
checkpoint:
  stateDir: $WORK_DIR/state/postgres-to-pgvector
EOF

cat > "$MYSQL_TO_QDRANT" <<EOF
name: connector-e2e-mysql-to-qdrant
source:
  type: mysql
  url: jdbc:mysql://127.0.0.1:$MYSQL_PORT/kuaia
  userEnv: KUAIA_MYSQL_USER
  passwordEnv: KUAIA_MYSQL_PASSWORD
  query: select id, content from documents order by id
  fetchSize: 1000
transforms:
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: qdrant
  url: $QDRANT_URL
  collection: kuaia_e2e_mysql_docs
  idField: id
  vectorField: embedding
  payloadFields: [id, content]
  wait: true
  timeoutMs: 30000
checkpoint:
  stateDir: $WORK_DIR/state/mysql-to-qdrant
EOF

if needs_qdrant; then
  wait_for_qdrant
fi
if needs_postgres; then
  wait_for_postgres
fi
if needs_mysql; then
  wait_for_mysql
fi
if needs_minio; then
  wait_for_minio
fi
if case_selected s3-qdrant; then
  setup_minio
fi
if needs_milvus; then
  wait_for_milvus
fi

if case_selected file-qdrant; then
  create_qdrant_collection kuaia_e2e_article_chunks
fi
if case_selected document-directory-qdrant; then
  create_qdrant_collection kuaia_e2e_document_directory_docs
fi
if case_selected duckdb-qdrant; then
  create_qdrant_collection kuaia_e2e_duckdb_docs
fi
if case_selected s3-qdrant; then
  create_qdrant_collection kuaia_e2e_s3_docs
fi
if case_selected postgres-qdrant; then
  create_qdrant_collection kuaia_e2e_pg_docs
fi
if case_selected mysql-qdrant; then
  create_qdrant_collection kuaia_e2e_mysql_docs
fi
if case_selected file-milvus; then
  create_milvus_collection kuaia_e2e_milvus_docs
  load_milvus_collection kuaia_e2e_milvus_docs
fi

if case_selected file-qdrant; then
  KUAIA_POSTGRES_USER=kuaia \
  KUAIA_POSTGRES_PASSWORD=kuaia \
  KUAIA_MYSQL_USER=kuaia \
  KUAIA_MYSQL_PASSWORD=kuaia \
  run_pipeline_with_summary \
    connector-e2e-jsonl-chunk-to-qdrant \
    "$FILE_TO_QDRANT" \
    "$WORK_DIR/summaries/local-jsonl-chunk-to-qdrant.json" \
    2 \
    6
  require_qdrant_count kuaia_e2e_article_chunks 6
fi

if case_selected document-directory-qdrant; then
  run_pipeline_with_summary \
    connector-e2e-document-directory-to-qdrant \
    "$DOCUMENT_DIRECTORY_TO_QDRANT" \
    "$WORK_DIR/summaries/document-directory-to-qdrant.json" \
    2 \
    2
  require_qdrant_count kuaia_e2e_document_directory_docs 2
fi

if case_selected file-milvus; then
  KUAIA_MILVUS_TOKEN=$MILVUS_TOKEN \
  run_pipeline_with_summary \
    connector-e2e-file-to-milvus \
    "$FILE_TO_MILVUS" \
    "$WORK_DIR/summaries/file-to-milvus.json" \
    2 \
    2
  require_milvus_count kuaia_e2e_milvus_docs 2
fi

if case_selected duckdb-qdrant; then
  run_pipeline_with_summary \
    connector-e2e-duckdb-to-qdrant \
    "$DUCKDB_TO_QDRANT" \
    "$WORK_DIR/summaries/duckdb-to-qdrant.json" \
    2 \
    2
  require_qdrant_count kuaia_e2e_duckdb_docs 2
fi

if case_selected s3-qdrant; then
  KUAIA_S3_ACCESS_KEY=minioadmin \
  KUAIA_S3_SECRET_KEY=minioadmin \
  run_pipeline_with_summary \
    connector-e2e-s3-to-qdrant \
    "$S3_TO_QDRANT" \
    "$WORK_DIR/summaries/s3-to-qdrant.json" \
    2 \
    2
  require_qdrant_count kuaia_e2e_s3_docs 2
fi

if case_selected postgres-qdrant; then
  KUAIA_POSTGRES_USER=kuaia \
  KUAIA_POSTGRES_PASSWORD=kuaia \
  KUAIA_MYSQL_USER=kuaia \
  KUAIA_MYSQL_PASSWORD=kuaia \
  run_pipeline_with_summary \
    connector-e2e-postgres-to-qdrant \
    "$POSTGRES_TO_QDRANT" \
    "$WORK_DIR/summaries/postgres-to-qdrant.json" \
    2 \
    2
  require_qdrant_count kuaia_e2e_pg_docs 2
fi

if case_selected postgres-pgvector; then
  KUAIA_POSTGRES_USER=kuaia \
  KUAIA_POSTGRES_PASSWORD=kuaia \
  run_pipeline_with_summary \
    connector-e2e-postgres-to-pgvector \
    "$POSTGRES_TO_PGVECTOR" \
    "$WORK_DIR/summaries/postgres-to-pgvector.json" \
    2 \
    2
  require_postgres_count document_vectors 2
fi

if case_selected mysql-qdrant; then
  KUAIA_POSTGRES_USER=kuaia \
  KUAIA_POSTGRES_PASSWORD=kuaia \
  KUAIA_MYSQL_USER=kuaia \
  KUAIA_MYSQL_PASSWORD=kuaia \
  run_pipeline_with_summary \
    connector-e2e-mysql-to-qdrant \
    "$MYSQL_TO_QDRANT" \
    "$WORK_DIR/summaries/mysql-to-qdrant.json" \
    3 \
    3
  require_qdrant_count kuaia_e2e_mysql_docs 3
fi

echo "Connector e2e case $SELECTED_CASE passed. Work dir: $WORK_DIR"
