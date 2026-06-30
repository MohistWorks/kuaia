package com.kuaia.engine;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import com.kuaia.engine.pipeline.PipelineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorServerTest {

    @Test
    void startBindsEphemeralPortAndCloseIsIdempotent() throws Exception {
        CoordinatorServer server = new CoordinatorServer(new InMemoryStateStore(), 30_000L);
        server.start(0);
        assertTrue(server.port() > 0, "ephemeral port should be assigned");
        server.close();
        server.close(); // second close must be a no-op, not throw
    }

    @Test
    void submitPersistsCreatedTasks(@TempDir Path tmp) throws Exception {
        Path input = tmp.resolve("in.csv");
        Files.write(input, String.join("\n", "id", "1", "2", "3").getBytes(StandardCharsets.UTF_8));
        Path output = tmp.resolve("out.csv");
        PipelineConfig cfg = new PipelineConfig(
                "submit-demo",
                new PipelineConfig.SourceConfig("file", input.toString(), "csv"),
                new PipelineConfig.SinkConfig("file", output.toString(), "csv", "overwrite"),
                new PipelineConfig.CheckpointConfig(null));

        CoordinatorServer server = new CoordinatorServer(new InMemoryStateStore(), 30_000L);
        try {
            JobInstance job = server.submit(cfg, 4);
            assertFalse(job.getTaskIds().isEmpty(), "submit should plan at least one task");
            for (String taskId : job.getTaskIds()) {
                assertEquals(TaskState.CREATED, server.stateStore().getTask(taskId).getState());
            }
        } finally {
            server.close();
        }
    }
}
