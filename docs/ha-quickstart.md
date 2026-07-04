# High-availability quickstart (multi-coordinator cluster)

This guide runs a **highly available** coordinator cluster: three coordinators replicate all
job/task/worker state through Raft, only the elected leader dispatches, and a leader crash is
survived — a new leader takes over and the job keeps running. Workers find the leader on their own,
and you can grow or shrink the cluster at runtime.

It builds on the single-coordinator [`distributed-quickstart.md`](distributed-quickstart.md). Every
command here is localhost-only with fixed ports; change the ports if they are occupied.

For a one-command version of steps 1–5, run
[`examples/cluster-ha-demo/run.sh`](../examples/cluster-ha-demo/run.sh).

## 1. Build the CLI jar

    export JAVA_HOME=<your JDK 21>
    mvn -pl kuaia-common,kuaia-connectors install -DskipTests
    mvn -pl kuaia-engine package -DskipTests

This produces `kuaia-engine/target/kuaia-engine-<version>-cli.jar`. The commands below assume
`JAR=kuaia-engine/target/kuaia-engine-*-cli.jar`.

## 2. Start a 3-node HA cluster

A coordinator enters HA mode when `--raft-peers` is present. Each node needs a unique `--node-id`
that appears in the shared peer list, its own `--state-dir` (Raft + RocksDB storage), and a
worker-facing gRPC `--port`. The peer list uses `id@host:raftPort` (the Raft port is separate from
the gRPC port).

Terminal A — node n1:

    java -jar $JAR coordinator --port 9001 --state-dir /tmp/kuaia-n1 \
        --node-id n1 --raft-peers n1@127.0.0.1:6001,n2@127.0.0.1:6002,n3@127.0.0.1:6003

Terminal B — node n2:

    java -jar $JAR coordinator --port 9002 --state-dir /tmp/kuaia-n2 \
        --node-id n2 --raft-peers n1@127.0.0.1:6001,n2@127.0.0.1:6002,n3@127.0.0.1:6003

Terminal C — node n3:

    java -jar $JAR coordinator --port 9003 --state-dir /tmp/kuaia-n3 \
        --node-id n3 --raft-peers n1@127.0.0.1:6001,n2@127.0.0.1:6002,n3@127.0.0.1:6003

The three nodes elect a leader within a couple of seconds.

## 3. Start a worker with the full coordinator list

Give the worker **all** coordinator gRPC addresses. It probes them in order, stays on the one that
answers as leader, and re-probes automatically if that connection drops — so you never point a worker
at a specific node.

    java -jar $JAR worker --id w1 \
        --coordinator 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003

## 4. Inspect the cluster

    java -jar $JAR cluster info --raft-peers n1@127.0.0.1:6001,n2@127.0.0.1:6002,n3@127.0.0.1:6003

Output shows the elected leader and each member's role:

    leader: n3
    n1 127.0.0.1:6001 FOLLOWER
    n2 127.0.0.1:6002 FOLLOWER
    n3 127.0.0.1:6003 LEADER

## 5. Submit a job and watch it complete

Submit and query status against **any** node — writes and linearizable reads are routed to the leader
through Raft.

    java -jar $JAR submit --coordinator 127.0.0.1:9001 -f examples/cluster-ha-demo/pipeline.yaml
    # -> Submitted job cluster-ha-demo-<ts> with 1 tasks

    java -jar $JAR status --coordinator 127.0.0.1:9001 --job cluster-ha-demo-<ts>
    # -> Job cluster-ha-demo-<ts> state=COMPLETED completed=1/1 failed=0 cancelled=0

(Adjust `pipeline.yaml`'s `source.path`/`sink.path` to absolute paths, or run from a directory where
the relative paths resolve.)

## 6. Survive a leader crash

Find the current leader with `cluster info`, then stop that coordinator process (Ctrl-C in its
terminal, or `kill` its PID). Within a couple of seconds:

- the survivors elect a new leader — `cluster info` now shows the killed node as `UNREACHABLE` and a
  different node as `LEADER`;
- the worker's stream to the old leader drops, so it re-probes the list and reconnects to the new
  leader on its own;
- a job submitted to any surviving node still runs to `COMPLETED`, because the replicated state and
  the new leader's dispatch loop carry on.

    java -jar $JAR cluster info --raft-peers n1@127.0.0.1:6001,n2@127.0.0.1:6002,n3@127.0.0.1:6003
    java -jar $JAR submit --coordinator 127.0.0.1:9002 -f examples/cluster-ha-demo/pipeline.yaml
    java -jar $JAR status --coordinator 127.0.0.1:9002 --job <job-id>

## 7. Grow and shrink the cluster at runtime

Add a node in two steps: start it (its `--raft-peers` includes itself; it idles outside the quorum
until the cluster knows about it), then commit the membership change.

    # start n4 (note the 4-entry peer list)
    java -jar $JAR coordinator --port 9004 --state-dir /tmp/kuaia-n4 \
        --node-id n4 --raft-peers n1@127.0.0.1:6001,n2@127.0.0.1:6002,n3@127.0.0.1:6003,n4@127.0.0.1:6004

    # pull it into the quorum
    java -jar $JAR cluster add-node \
        --raft-peers n1@127.0.0.1:6001,n2@127.0.0.1:6002,n3@127.0.0.1:6003 \
        --node n4@127.0.0.1:6004
    # -> members: n1@... n2@... n3@... n4@...

Remove a node (if it is the leader, leadership is transferred first, automatically). Membership is
based on the cluster's live configuration, so an out-of-date `--raft-peers` list cannot silently
evict a member it forgets to list.

    java -jar $JAR cluster remove-node \
        --raft-peers n1@127.0.0.1:6001,n2@127.0.0.1:6002,n3@127.0.0.1:6003,n4@127.0.0.1:6004 \
        --node-id n2

The removed coordinator can then be stopped.

## Delivery guarantee

The `file` sink with `mode: overwrite` is exactly-once across a same-filesystem resume: a task
recovered after a crash truncates its output back to the committed checkpoint and re-appends, so rows
are never duplicated or lost. Vector sinks are effectively-once via upsert on the required `idField`.
See [`connector-development.md`](connector-development.md) for the full delivery-guarantee notes.
