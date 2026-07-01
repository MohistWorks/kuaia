package com.kuaia.engine;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.rpc.CoordinatorServiceGrpc;
import com.kuaia.common.rpc.JobStatusRequest;
import com.kuaia.common.rpc.JobStatusResponse;
import com.kuaia.common.rpc.SubmitJobRequest;
import com.kuaia.common.rpc.SubmitJobResponse;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;
import com.kuaia.engine.pipeline.PipelineConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Milestone end-to-end: a real CoordinatorServer (real gRPC + live dispatch loop) and a real
 * WorkerNode (via WorkerRunner) run a file -> identity -> file job to COMPLETED across the wire.
 */
class CoordinatorWorkerE2ETest {

    @Test
    void coordinatorDispatchesToRealWorkerUntilJobCompletes(@TempDir Path tmp) throws Exception {
        Path input = tmp.resolve("in.csv");
        Files.write(input, String.join("\n", "id", "1", "2", "3", "4", "5").getBytes(StandardCharsets.UTF_8));
        Path output = tmp.resolve("out.csv");
        PipelineConfig cfg = new PipelineConfig(
                "e2e-file-to-file",
                new PipelineConfig.SourceConfig("file", input.toString(), "csv"),
                new PipelineConfig.SinkConfig("file", output.toString(), "csv", "overwrite"),
                new PipelineConfig.CheckpointConfig(null));

        CoordinatorServer server = new CoordinatorServer(new RocksDbStateStore(tmp.resolve("coord-state")), 30_000L);
        WorkerRunner worker = new WorkerRunner("worker-e2e-" + System.nanoTime());
        try {
            server.start(0);
            JobInstance job = server.submit(cfg, 4);
            worker.start("127.0.0.1", server.port());

            assertTrue(
                    await(() -> {
                        JobInstance j = server.stateStore().getJob(job.getJobId());
                        return j != null && j.getState() == TaskState.COMPLETED;
                    }, 20_000),
                    "job should reach COMPLETED");

            assertTrue(Files.exists(output), "output file should exist");
            List<String> lines = Files.readAllLines(output, StandardCharsets.UTF_8);
            assertEquals(6, lines.size(), "expected header + 5 data rows");
            assertEquals("id", lines.get(0));
        } finally {
            worker.close();
            server.close();
        }
    }

    @Test
    void coordinatorRecoversPersistedTasksOnRestart(@TempDir Path tmp) throws Exception {
        Path input = tmp.resolve("in.csv");
        Files.write(input, String.join("\n", "id", "1", "2", "3").getBytes(StandardCharsets.UTF_8));
        Path output = tmp.resolve("out.csv");
        PipelineConfig cfg = new PipelineConfig(
                "e2e-recover",
                new PipelineConfig.SourceConfig("file", input.toString(), "csv"),
                new PipelineConfig.SinkConfig("file", output.toString(), "csv", "overwrite"),
                new PipelineConfig.CheckpointConfig(null));
        Path stateDir = tmp.resolve("coord-state");

        // First coordinator: submit, start (no worker connects), then shut down — tasks stay CREATED.
        String jobId;
        CoordinatorServer server1 = new CoordinatorServer(new RocksDbStateStore(stateDir), 30_000L);
        try {
            server1.start(0);
            JobInstance job = server1.submit(cfg, 4);
            jobId = job.getJobId();
            for (String taskId : job.getTaskIds()) {
                assertEquals(TaskState.CREATED, server1.stateStore().getTask(taskId).getState());
            }
        } finally {
            server1.close();
        }

        // Second coordinator: same state dir, NO --submit. The dispatch loop recovers the persisted
        // CREATED tasks and runs them once a worker connects.
        CoordinatorServer server2 = new CoordinatorServer(new RocksDbStateStore(stateDir), 30_000L);
        WorkerRunner worker = new WorkerRunner("worker-recover-" + System.nanoTime());
        try {
            server2.start(0);
            worker.start("127.0.0.1", server2.port());

            assertTrue(
                    await(() -> {
                        JobInstance j = server2.stateStore().getJob(jobId);
                        return j != null && j.getState() == TaskState.COMPLETED;
                    }, 20_000),
                    "recovered job should reach COMPLETED");

            List<String> lines = Files.readAllLines(output, StandardCharsets.UTF_8);
            assertEquals(4, lines.size(), "expected header + 3 data rows");
        } finally {
            worker.close();
            server2.close();
        }
    }

    @Test
    void runtimeSubmitViaRpcRunsJobToCompletion(@TempDir Path tmp) throws Exception {
        Path input = tmp.resolve("in.csv");
        Files.write(input, String.join("\n", "id", "1", "2", "3", "4", "5").getBytes(StandardCharsets.UTF_8));
        Path output = tmp.resolve("out.csv");
        String yaml = String.join("\n",
                "name: e2e-runtime-submit",
                "source:", "  type: file", "  path: " + input, "  format: csv",
                "sink:", "  type: file", "  path: " + output, "  format: csv", "  mode: overwrite");

        CoordinatorServer server = new CoordinatorServer(new RocksDbStateStore(tmp.resolve("coord-state")), 30_000L);
        WorkerRunner worker = new WorkerRunner("worker-runtime-" + System.nanoTime());
        ManagedChannel channel = null;
        try {
            server.start(0);
            worker.start("127.0.0.1", server.port());
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port()).usePlaintext().build();
            CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stub = CoordinatorServiceGrpc.newBlockingStub(channel);

            SubmitJobResponse submit = stub.submitJob(SubmitJobRequest.newBuilder()
                    .setPipelineYaml(yaml).setMaxParallelism(4).build());
            assertTrue(submit.getSuccess(), submit.getError());
            String jobId = submit.getJobId();
            assertTrue(submit.getTaskCount() > 0, "expected planned tasks");

            CoordinatorServiceGrpc.CoordinatorServiceBlockingStub pollStub = stub;
            assertTrue(await(() -> {
                JobStatusResponse st = pollStub.getJobStatus(JobStatusRequest.newBuilder().setJobId(jobId).build());
                return st.getFound() && "COMPLETED".equals(st.getJob().getState());
            }, 20_000), "job should reach COMPLETED via status RPC");

            assertEquals(6, Files.readAllLines(output, StandardCharsets.UTF_8).size(), "header + 5 rows");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            worker.close();
            server.close();
        }
    }

    /** Bounded poll: true as soon as the condition holds, false if it never does before timeout. */
    static boolean await(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return condition.getAsBoolean();
    }
}
