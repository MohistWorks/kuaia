package com.kuaia.engine;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;
import com.kuaia.engine.pipeline.PipelineConfig;
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
                    await(() -> server.stateStore().getJob(job.getJobId()).getState() == TaskState.COMPLETED, 20_000),
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
