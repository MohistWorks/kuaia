# Cluster HA demo

A one-command, zero-dependency demonstration of Kuaia's high-availability distributed engine on
localhost: a 3-node coordinator cluster, a worker that discovers the leader on its own, and a
`file -> file` job submitted at runtime and run to completion.

## Run it

Build the CLI jar once, then run the script:

    export JAVA_HOME=<your JDK 21>
    mvn -pl kuaia-common,kuaia-connectors install -DskipTests
    mvn -pl kuaia-engine package -DskipTests
    sh examples/cluster-ha-demo/run.sh

Or point it at an existing jar:

    KUAIA_JAR=path/to/kuaia-engine-<version>-cli.jar sh examples/cluster-ha-demo/run.sh

## What `run.sh` does

1. Starts three coordinators (`n1`, `n2`, `n3`) in HA mode — gRPC ports 9001–9003, Raft ports
   6001–6003, each with its own state directory under a temp folder.
2. Starts one worker with **all three** coordinator addresses; it probes them and stays on the
   leader (auto-discovery).
3. Polls `kuaia cluster info` until a leader is elected and prints the members.
4. Submits the demo pipeline and polls `kuaia status` until the job is `COMPLETED`.
5. Prints the output file, then cleans up every background process and the temp directory on exit.

Everything runs on localhost with fixed ports; edit the `PEERS`/`GRPC` lines in `run.sh` if those
ports are occupied.

## Try failover and scaling by hand

The script covers the happy path. To see the HA behavior directly, follow the step-by-step guide in
[`docs/ha-quickstart.md`](../../docs/ha-quickstart.md), which walks through:

- **Leader crash** — find the leader with `kuaia cluster info`, stop that coordinator, and watch the
  worker reconnect to the new leader while jobs keep completing.
- **Grow the cluster** — start a fourth coordinator and `kuaia cluster add-node` it into the quorum.
- **Shrink the cluster** — `kuaia cluster remove-node` (leadership is transferred first if you remove
  the leader).

## Files

- `pipeline.yaml` — the demo `file -> file` pipeline (paths are rewritten to the temp dir by `run.sh`).
- `input.csv` — sample input rows.
- `run.sh` — the launcher described above.
