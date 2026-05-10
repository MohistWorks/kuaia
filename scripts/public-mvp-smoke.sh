#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
KUAIA_CMD=${KUAIA:-"$ROOT_DIR/bin/kuaia"}
RUN_ID=$(date +%Y%m%d%H%M%S)-$$
WORK_DIR="$ROOT_DIR/.kuaia/public-mvp-smoke/$RUN_ID"

mkdir -p "$WORK_DIR/data" "$WORK_DIR/output" "$WORK_DIR/state"
cp "$ROOT_DIR/examples/data/users.csv" "$WORK_DIR/data/users.csv"
cp "$ROOT_DIR/examples/data/documents.csv" "$WORK_DIR/data/documents.csv"
cp "$ROOT_DIR/examples/data/users-with-bad-row.csv" "$WORK_DIR/data/users-with-bad-row.csv"

CSV_TO_FILE="$WORK_DIR/csv-to-file.yaml"
CSV_TO_VECTOR="$WORK_DIR/csv-to-vector.yaml"
SKIP_BAD_RECORDS="$WORK_DIR/skip-bad-records.yaml"

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

run_pipeline "CSV to transformed file" "$CSV_TO_FILE"

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

run_pipeline "CSV to mock vector" "$CSV_TO_VECTOR"
run_pipeline "Skip malformed CSV records" "$SKIP_BAD_RECORDS"

printf '\nPublic MVP smoke passed. Work dir: %s\n' "$WORK_DIR"
