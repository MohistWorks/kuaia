# Distributed quickstart (coordinator + worker)

Run a pipeline across two processes: a coordinator (gRPC server + dispatch loop, with persistent
state) and a worker that executes the dispatched tasks.

## 1. Build the CLI jar

    export JAVA_HOME=<your JDK 21>
    mvn -pl kuaia-common,kuaia-connectors install -DskipTests
    mvn -pl kuaia-engine package -DskipTests

This produces `kuaia-engine/target/kuaia-engine-<version>-cli.jar`.

## 2. Terminal A — coordinator (persistent state + submit a job at startup)

    java -jar kuaia-engine/target/kuaia-engine-*-cli.jar coordinator \
        --port 9000 \
        --state-dir /tmp/kuaia-coord \
        --submit examples/cluster-demo/pipeline.yaml

The coordinator enumerates the pipeline's source splits into CREATED tasks, persists them under
`--state-dir`, then listens on `--port` and dispatches as workers connect.

## 3. Terminal B — worker

    java -jar kuaia-engine/target/kuaia-engine-*-cli.jar worker \
        --id w1 \
        --coordinator 127.0.0.1:9000

The worker connects, receives task assignments over the stream, executes them, and reports results.

## 4. Observe

- Terminal A logs the job progressing to `COMPLETED`.
- The sink output appears at `examples/cluster-demo/output.csv`.

## Restarting the coordinator

The coordinator's state under `--state-dir` survives restarts. To resume after a crash, restart with
the **same** `--state-dir` and **without** `--submit` — the dispatch loop recovers the persisted
tasks and re-dispatches them. Passing `--submit` again would submit a second, duplicate job.

## Runtime submission (submit + status while the coordinator runs)

Instead of submitting a job at coordinator startup with `--submit`, you can start the coordinator
empty and submit jobs to it over gRPC at runtime, then query their status — no restart required.

Terminal A — coordinator, started **without** `--submit`:

    java -jar kuaia-engine/target/kuaia-engine-*-cli.jar coordinator \
        --port 9000 \
        --state-dir /tmp/kuaia-coord

Terminal B — worker:

    java -jar kuaia-engine/target/kuaia-engine-*-cli.jar worker \
        --id w1 \
        --coordinator 127.0.0.1:9000

Terminal C — submit a job and watch it, using the CLI as a client:

    java -jar kuaia-engine/target/kuaia-engine-*-cli.jar submit --coordinator 127.0.0.1:9000 -f examples/cluster-demo/pipeline.yaml
    java -jar kuaia-engine/target/kuaia-engine-*-cli.jar status --coordinator 127.0.0.1:9000
    java -jar kuaia-engine/target/kuaia-engine-*-cli.jar status --coordinator 127.0.0.1:9000 --job <job-id>

- `submit` sends the pipeline YAML to the coordinator, which plans it into tasks and returns the
  new **job id** and the **task count**. The dispatch loop then hands those tasks to connected
  workers just like a startup `--submit` job.
- `status` with no `--job` lists **all** jobs the coordinator knows about, with their states.
- `status --job <job-id>` shows a single job's aggregate **state** and its per-state task counts —
  poll it until the job reads `COMPLETED`.
