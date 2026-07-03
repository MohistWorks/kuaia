package com.kuaia.engine;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.worker.WorkerNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.kuaia.engine.CoordinatorWorkerE2ETest.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A worker configured with [follower, leader] probes past the follower (which answers
 * {@code HelloAck(false)} and closes the stream) and lands on the leader, where a submitted job runs
 * to COMPLETED — no operator ever points the worker at the leader.
 */
class WorkerLeaderDiscoveryTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void workerProbesPastFollowerAndExecutesOnLeader(@TempDir Path tmp) throws Exception {
        Path input = tmp.resolve("in.csv");
        Files.write(input, String.join("\n", "id", "1", "2", "3").getBytes(StandardCharsets.UTF_8));
        Path output = tmp.resolve("out.csv");
        PipelineConfig cfg = new PipelineConfig(
                "leader-discovery",
                new PipelineConfig.SourceConfig("file", input.toString(), "csv"),
                new PipelineConfig.SinkConfig("file", output.toString(), "csv", "overwrite"),
                new PipelineConfig.CheckpointConfig(null));

        CoordinatorServer follower = new CoordinatorServer(new InMemoryStateStore(), 30_000L, () -> false);
        CoordinatorServer leader = new CoordinatorServer(new InMemoryStateStore(), 30_000L, () -> true);
        WorkerRunner worker = new WorkerRunner("worker-discovery-" + System.nanoTime());
        try {
            follower.start(0);
            leader.start(0);
            JobInstance job = leader.submit(cfg, 4);

            // Follower listed first: the worker must probe past it to find the leader.
            worker.start(List.of(
                    new WorkerNode.HostPort("127.0.0.1", follower.port()),
                    new WorkerNode.HostPort("127.0.0.1", leader.port())));

            assertTrue(
                    await(() -> {
                        JobInstance j = leader.stateStore().getJob(job.getJobId());
                        return j != null && j.getState() == TaskState.COMPLETED;
                    }, 20_000),
                    "job should complete on the leader the worker discovered");
            assertTrue(Files.exists(output), "output file should exist");
        } finally {
            worker.close();
            leader.close();
            follower.close();
        }
    }
}
