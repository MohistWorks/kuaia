package com.kuaia.engine;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;
import com.kuaia.engine.pipeline.LocalPipelineCheckpointStore;
import com.kuaia.engine.pipeline.PipelineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A file-sink pipeline resumed from a checkpoint that lags the rows already written to the file must
 * produce each row exactly once — the retry truncates the file back to the checkpoint boundary and
 * re-appends, rather than duplicating (old append behavior) or losing (old overwrite behavior).
 */
class ExactlyOnceResumeTest {

    private static final PrintStream DISCARD = new PrintStream(OutputStream.nullOutputStream());

    @Test
    void fileSinkResumeProducesEachRowExactlyOnce(@TempDir Path tmp) throws Exception {
        Path input = tmp.resolve("in.csv");
        Files.write(input, String.join("\n", "id", "1", "2", "3", "4", "5", "6").getBytes(StandardCharsets.UTF_8));
        Path output = tmp.resolve("out.csv");
        Path stateDir = tmp.resolve("state");
        PipelineConfig cfg = new PipelineConfig(
                "eo-resume",
                new PipelineConfig.SourceConfig("file", input.toString(), "csv"),
                new PipelineConfig.SinkConfig("file", output.toString(), "csv", "overwrite"),
                new PipelineConfig.CheckpointConfig(stateDir.toString()));

        // First run to completion: the identity pipeline has batch size 1, so a commit lands at every
        // sequence id and the file holds header + rows 1..6.
        new LocalPipelineRunner().run(cfg, DISCARD);
        assertEquals(7, Files.readAllLines(output, StandardCharsets.UTF_8).size(), "header + 6 rows");

        // Simulate a crash that wrote rows past the durable checkpoint: rewind the persisted checkpoint
        // to seq 3 while the file still holds all six rows, then re-run.
        rewindCheckpoint(stateDir, "eo-resume", 3L);
        new LocalPipelineRunner().run(cfg, DISCARD);

        assertEquals(List.of("id", "1", "2", "3", "4", "5", "6"),
                Files.readAllLines(output, StandardCharsets.UTF_8),
                "each row exactly once after resume — no duplicates, no loss");
    }

    /** Persist a RUNNING task whose checkpoint sits at {@code seq}, so the next run resumes from there. */
    private void rewindCheckpoint(Path stateDir, String pipelineName, long seq) throws Exception {
        String taskId = LocalPipelineCheckpointStore.taskIdFor(pipelineName);
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord rewound = TaskRecord.created(pipelineName, taskId)
                    .dispatching("local-worker", "local-attempt-1", Long.MAX_VALUE)
                    .running()
                    .checkpoint("local-attempt-1", seq);
            store.saveTask(rewound);
        }
    }
}
