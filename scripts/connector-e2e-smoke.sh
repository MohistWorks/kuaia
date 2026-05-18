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

compose() {
  docker compose -p "$COMPOSE_PROJECT" -f "$E2E_COMPOSE" "$@"
}

cleanup() {
  status=$?
  compose down -v >/dev/null 2>&1 || true
  exit "$status"
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

trap cleanup EXIT INT TERM

mkdir -p "$WORK_DIR/data" "$WORK_DIR/state" "$WORK_DIR/summaries"
cp "$ROOT_DIR/examples/data/articles.jsonl" "$WORK_DIR/data/articles.jsonl"

FILE_TO_QDRANT="$WORK_DIR/local-jsonl-chunk-to-qdrant.yaml"
POSTGRES_TO_QDRANT="$WORK_DIR/postgres-to-qdrant.yaml"
MYSQL_TO_QDRANT="$WORK_DIR/mysql-to-qdrant.yaml"

cat > "$E2E_COMPOSE" <<EOF
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: kuaia
      POSTGRES_USER: kuaia
      POSTGRES_PASSWORD: kuaia
    ports:
      - "127.0.0.1::5432"
    volumes:
      - "$ROOT_DIR/examples/postgres/init:/docker-entrypoint-initdb.d:ro"
      - postgres-data:/var/lib/postgresql/data

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

  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "127.0.0.1::6333"
    volumes:
      - qdrant-data:/qdrant/storage

volumes:
  postgres-data:
  qdrant-data:
EOF

echo "Starting connector e2e services with Compose project $COMPOSE_PROJECT..."
compose up -d

POSTGRES_PORT=$(service_port postgres 5432)
MYSQL_PORT=$(service_port mysql 3306)
QDRANT_PORT=$(service_port qdrant 6333)
QDRANT_URL="http://127.0.0.1:$QDRANT_PORT"

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

wait_for_qdrant
wait_for_postgres
wait_for_mysql

create_qdrant_collection kuaia_e2e_article_chunks
create_qdrant_collection kuaia_e2e_pg_docs
create_qdrant_collection kuaia_e2e_mysql_docs

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

echo "Connector e2e smoke passed. Work dir: $WORK_DIR"
