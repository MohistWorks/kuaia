#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
KUAIA_CMD=${KUAIA:-"$ROOT_DIR/bin/kuaia"}
RUN_ID=$(date +%Y%m%d%H%M%S)-$$
WORK_DIR="$ROOT_DIR/.kuaia/public-mvp-smoke/$RUN_ID"

mkdir -p "$WORK_DIR/data" "$WORK_DIR/output" "$WORK_DIR/state"
cp "$ROOT_DIR/examples/data/users.csv" "$WORK_DIR/data/users.csv"
cp "$ROOT_DIR/examples/data/quoted-documents.csv" "$WORK_DIR/data/quoted-documents.csv"
cp "$ROOT_DIR/examples/data/documents.csv" "$WORK_DIR/data/documents.csv"
cp "$ROOT_DIR/examples/data/documents.jsonl" "$WORK_DIR/data/documents.jsonl"
cp "$ROOT_DIR/examples/data/articles.jsonl" "$WORK_DIR/data/articles.jsonl"
cp "$ROOT_DIR/examples/data/users-with-bad-row.csv" "$WORK_DIR/data/users-with-bad-row.csv"

CSV_TO_FILE="$WORK_DIR/csv-to-file.yaml"
QUOTED_CSV_TO_FILE="$WORK_DIR/quoted-csv-to-file.yaml"
CSV_TO_VECTOR="$WORK_DIR/csv-to-vector.yaml"
JSONL_TO_FILE="$WORK_DIR/jsonl-to-file.yaml"
JSONL_TO_VECTOR="$WORK_DIR/jsonl-to-vector.yaml"
JSONL_CHUNK_TO_VECTOR="$WORK_DIR/jsonl-chunk-to-vector.yaml"
SKIP_BAD_RECORDS="$WORK_DIR/skip-bad-records.yaml"
FATAL_BAD_RECORDS="$WORK_DIR/fatal-bad-records.yaml"

cat > "$CSV_TO_FILE" <<EOF
name: public-mvp-csv-to-file
source:
  type: file
  path: data/users.csv
  format: csv
transforms:
  - type: select
    fields: [id, name]
  - type: rename
    from: name
    to: user_name
sink:
  type: file
  path: $WORK_DIR/output/users.csv
  format: csv
  mode: overwrite
checkpoint:
  stateDir: $WORK_DIR/state/csv-to-file
EOF

cat > "$CSV_TO_VECTOR" <<EOF
name: public-mvp-csv-to-vector
source:
  type: file
  path: data/documents.csv
  format: csv
transforms:
  - type: select
    fields: [id, content]
  - type: filter
    field: content
    op: not-empty
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: mock-vector
checkpoint:
  stateDir: $WORK_DIR/state/csv-to-vector
EOF

cat > "$JSONL_TO_FILE" <<EOF
name: public-mvp-jsonl-to-file
source:
  type: file
  path: data/documents.jsonl
  format: jsonl
transforms:
  - type: select
    fields: [id, content]
  - type: trim
    field: content
  - type: lowercase
    field: content
  - type: replace
    field: content
    target: ph
    replacement: f
  - type: filter
    field: content
    op: not-empty
  - type: filter
    field: content
    op: min-length
    minLength: 4
  - type: filter
    field: content
    op: contains
    value: a
  - type: filter
    field: content
    op: starts-with
    value: a
  - type: filter
    field: content
    op: equals
    value: alfa
  - type: filter
    field: id
    op: less-than
    value: 2
sink:
  type: file
  path: $WORK_DIR/output/documents.jsonl
  format: jsonl
  mode: overwrite
checkpoint:
  stateDir: $WORK_DIR/state/jsonl-to-file
EOF

cat > "$QUOTED_CSV_TO_FILE" <<EOF
name: public-mvp-quoted-csv-to-file
source:
  type: file
  path: data/quoted-documents.csv
  format: csv
sink:
  type: file
  path: $WORK_DIR/output/quoted-documents.csv
  format: csv
  mode: overwrite
checkpoint:
  stateDir: $WORK_DIR/state/quoted-csv-to-file
EOF

cat > "$JSONL_TO_VECTOR" <<EOF
name: public-mvp-jsonl-to-vector
source:
  type: file
  path: data/documents.jsonl
  format: jsonl
transforms:
  - type: select
    fields: [id, content]
  - type: trim
    field: content
  - type: filter
    field: content
    op: not-empty
  - type: mock-embedding
    input: content
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: mock-vector
checkpoint:
  stateDir: $WORK_DIR/state/jsonl-to-vector
EOF

cat > "$JSONL_CHUNK_TO_VECTOR" <<EOF
name: public-mvp-jsonl-chunk-to-vector
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
  - type: mock-embedding
    input: chunk
    output: embedding
    dimensions: 4
    batchSize: 32
sink:
  type: mock-vector
checkpoint:
  stateDir: $WORK_DIR/state/jsonl-chunk-to-vector
EOF

cat > "$SKIP_BAD_RECORDS" <<EOF
name: public-mvp-skip-bad-records
source:
  type: file
  path: data/users-with-bad-row.csv
  format: csv
sink:
  type: console
errorPolicy:
  mode: skip-bad-records
checkpoint:
  stateDir: $WORK_DIR/state/skip-bad-records
EOF

cat > "$FATAL_BAD_RECORDS" <<EOF
name: public-mvp-fatal-bad-records
source:
  type: file
  path: data/users-with-bad-row.csv
  format: csv
sink:
  type: console
EOF

require_contains() {
  text=$1
  needle=$2
  case "$text" in
    *"$needle"*) ;;
    *)
      printf '%s\n' "Expected output to contain: $needle" >&2
      exit 1
      ;;
  esac
}

require_not_contains() {
  text=$1
  needle=$2
  case "$text" in
    *"$needle"*)
      printf '%s\n' "Expected output not to contain: $needle" >&2
      exit 1
      ;;
    *) ;;
  esac
}

run_pipeline() {
  name=$1
  file=$2
  printf '\n== %s ==\n' "$name"
  if ! output=$("$KUAIA_CMD" run -f "$file" 2>&1); then
    printf '%s\n' "$output"
    exit 1
  fi
  printf '%s\n' "$output"
  require_contains "$output" "Pipeline Finished."
  require_contains "$output" "Run Summary:"
  require_contains "$output" "taskState=COMPLETED"
}

run_pipeline_with_summary_json() {
  name=$1
  file=$2
  summary_json=$3
  printf '\n== %s ==\n' "$name"
  if ! output=$("$KUAIA_CMD" run -f "$file" --summary-json "$summary_json" 2>&1); then
    printf '%s\n' "$output"
    exit 1
  fi
  printf '%s\n' "$output"
  require_contains "$output" "Pipeline Finished."
  require_contains "$output" "Run Summary:"
  require_contains "$output" "taskState=COMPLETED"
  require_contains "$output" "Run Summary JSON: $summary_json"
  if [ ! -f "$summary_json" ]; then
    printf '%s\n' "Expected summary JSON to exist: $summary_json" >&2
    exit 1
  fi
}

run_expected_failure() {
  name=$1
  file=$2
  expected=$3
  printf '\n== %s ==\n' "$name"
  set +e
  output=$("$KUAIA_CMD" run -f "$file" 2>&1)
  status=$?
  set -e
  printf '%s\n' "$output"
  if [ "$status" -eq 0 ]; then
    printf '%s\n' "Expected pipeline to fail: $file" >&2
    exit 1
  fi
  require_contains "$output" "$expected"
  require_not_contains "$output" "Run Summary:"
}

CSV_SUMMARY_JSON="$WORK_DIR/output/csv-to-file-summary.json"
run_pipeline_with_summary_json "CSV to transformed file" "$CSV_TO_FILE" "$CSV_SUMMARY_JSON"

SUMMARY_JSON_CONTENT=$(cat "$CSV_SUMMARY_JSON")
require_contains "$SUMMARY_JSON_CONTENT" '"pipelineName":"public-mvp-csv-to-file"'
require_contains "$SUMMARY_JSON_CONTENT" '"rowsRead":2'
require_contains "$SUMMARY_JSON_CONTENT" '"rowsWritten":2'
require_contains "$SUMMARY_JSON_CONTENT" '"taskState":"COMPLETED"'

EXPECTED_USERS="$WORK_DIR/expected-users.csv"
{
  printf '%s\n' "id,user_name"
  printf '%s\n' "1,Alice"
  printf '%s\n' "2,Bob"
} > "$EXPECTED_USERS"

if ! diff -u "$EXPECTED_USERS" "$WORK_DIR/output/users.csv"; then
  printf '%s\n' "CSV file output did not match expected rows" >&2
  exit 1
fi

run_pipeline "Quoted CSV to file" "$QUOTED_CSV_TO_FILE"

EXPECTED_QUOTED_DOCUMENTS="$WORK_DIR/expected-quoted-documents.csv"
{
  printf '%s\n' "id,content"
  printf '%s\n' '1,"Alpha, ""Beta"""'
  printf '%s\n' '2,"Line one'
  printf '%s\n' 'Line two"'
} > "$EXPECTED_QUOTED_DOCUMENTS"

if ! diff -u "$EXPECTED_QUOTED_DOCUMENTS" "$WORK_DIR/output/quoted-documents.csv"; then
  printf '%s\n' "Quoted CSV file output did not match expected rows" >&2
  exit 1
fi

run_pipeline "JSONL to file" "$JSONL_TO_FILE"

EXPECTED_DOCUMENTS="$WORK_DIR/expected-documents.jsonl"
{
  printf '%s\n' '{"id":1,"content":"alfa"}'
} > "$EXPECTED_DOCUMENTS"

if ! diff -u "$EXPECTED_DOCUMENTS" "$WORK_DIR/output/documents.jsonl"; then
  printf '%s\n' "JSONL file output did not match expected rows" >&2
  exit 1
fi

run_pipeline "CSV to mock vector" "$CSV_TO_VECTOR"
run_pipeline "JSONL to mock vector" "$JSONL_TO_VECTOR"
run_pipeline "JSONL chunk to mock vector" "$JSONL_CHUNK_TO_VECTOR"
run_pipeline "Skip malformed CSV records" "$SKIP_BAD_RECORDS"
run_expected_failure "Fatal malformed CSV records" "$FATAL_BAD_RECORDS" "Source stage failed:"

printf '\nPublic MVP smoke passed. Work dir: %s\n' "$WORK_DIR"
