#!/usr/bin/env sh
# One-command HA demo: start a 3-node coordinator cluster + 1 worker on localhost, submit a
# file -> file pipeline, and watch it run to COMPLETED. Localhost only, fixed ports (change below if
# they are occupied), zero external dependencies. Cleans up all background processes on exit.
#
# Usage:
#   KUAIA_JAR=path/to/kuaia-engine-<version>-cli.jar sh examples/cluster-ha-demo/run.sh
# or build first (mvn -pl kuaia-engine package -DskipTests) and just run it.
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

# Resolve the CLI jar the same way bin/kuaia does.
JAR=${KUAIA_JAR:-}
if [ -z "$JAR" ]; then
  JAR=$(ls "$ROOT_DIR"/kuaia-engine/target/kuaia-engine-*-cli.jar 2>/dev/null | head -n 1 || true)
fi
if [ -z "$JAR" ] || [ ! -f "$JAR" ]; then
  echo "CLI jar not found. Build it first:" >&2
  echo "  mvn -pl kuaia-common,kuaia-connectors install -DskipTests" >&2
  echo "  mvn -pl kuaia-engine package -DskipTests" >&2
  echo "or set KUAIA_JAR=path/to/kuaia-engine-<version>-cli.jar" >&2
  exit 1
fi

PEERS="n1@127.0.0.1:6001,n2@127.0.0.1:6002,n3@127.0.0.1:6003"
GRPC="127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003"

WORK=$(mktemp -d)
PIDS=""
cleanup() {
  # shellcheck disable=SC2086
  [ -n "$PIDS" ] && kill $PIDS 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

kuaia() { java -jar "$JAR" "$@"; }

# Self-contained inputs/outputs under the temp dir.
cp "$SCRIPT_DIR/input.csv" "$WORK/input.csv"
cat > "$WORK/pipeline.yaml" <<YAML
name: cluster-ha-demo
source:
  type: file
  path: $WORK/input.csv
  format: csv
sink:
  type: file
  path: $WORK/output.csv
  format: csv
  mode: overwrite
YAML

echo "==> Starting 3-node HA coordinator cluster (gRPC 9001-9003, Raft 6001-6003)"
i=1
while [ "$i" -le 3 ]; do
  java -jar "$JAR" coordinator \
      --port "900$i" --state-dir "$WORK/n$i" --node-id "n$i" --raft-peers "$PEERS" \
      > "$WORK/n$i.log" 2>&1 &
  PIDS="$PIDS $!"
  i=$((i + 1))
done

echo "==> Starting a worker with the full coordinator list (it probes for the leader)"
java -jar "$JAR" worker --id w1 --coordinator "$GRPC" > "$WORK/w1.log" 2>&1 &
PIDS="$PIDS $!"

echo "==> Waiting for a leader to be elected..."
leader=""
i=0
while [ "$i" -lt 30 ]; do
  info=$(kuaia cluster info --raft-peers "$PEERS" 2>/dev/null || true)
  leader=$(printf '%s\n' "$info" | sed -n 's/^leader: //p')
  if [ -n "$leader" ] && [ "$leader" != "(none)" ]; then
    break
  fi
  sleep 1
  i=$((i + 1))
done
if [ -z "$leader" ] || [ "$leader" = "(none)" ]; then
  echo "No leader was elected in time. Coordinator logs are under $WORK." >&2
  exit 1
fi
echo "$info"

echo "==> Submitting the demo pipeline"
submit_out=$(kuaia submit --coordinator 127.0.0.1:9001 -f "$WORK/pipeline.yaml" 2>&1)
echo "$submit_out"
job_id=$(printf '%s\n' "$submit_out" | sed -n 's/^Submitted job \([^ ]*\) with.*/\1/p')
if [ -z "$job_id" ]; then
  echo "Submit did not return a job id. See $WORK for logs." >&2
  exit 1
fi

echo "==> Polling status until COMPLETED"
done_ok=""
i=0
while [ "$i" -lt 40 ]; do
  st=$(kuaia status --coordinator 127.0.0.1:9001 --job "$job_id" 2>/dev/null || true)
  case "$st" in
    *state=COMPLETED*) echo "$st"; done_ok=1; break ;;
  esac
  sleep 1
  i=$((i + 1))
done
if [ -z "$done_ok" ]; then
  echo "Job $job_id did not reach COMPLETED in time. See $WORK for logs." >&2
  exit 1
fi

echo "==> Output file ($WORK/output.csv):"
cat "$WORK/output.csv"

echo ""
echo "HA demo succeeded: leader=$leader, job=$job_id COMPLETED."
echo "Try failover and scaling by hand — see examples/cluster-ha-demo/README.md."
