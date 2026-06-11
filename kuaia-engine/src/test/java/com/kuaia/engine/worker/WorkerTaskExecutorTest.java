package com.kuaia.engine.worker;

import com.google.protobuf.ByteString;
import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.rpc.AttemptStatus;
import com.kuaia.common.rpc.TaskAssignment;
import com.kuaia.common.rpc.TaskAttemptResult;
import com.kuaia.common.rpc.WorkerMessage;
import com.kuaia.engine.coordinator.planner.JobSubmissionService;
import com.kuaia.engine.pipeline.ConnectorFactory;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import com.kuaia.engine.worker.connector.v2.SourceEnumerator;
import com.kuaia.engine.worker.connector.v2.SourceSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerTaskExecutorTest {
    @TempDir
    Path tmp;

    @Test
    void executesFileToFileSplitAndReportsSuccess() throws Exception {
        // FileSource CSV: first line is the header; data rows are assigned seqIds starting at 1.
        Path input = tmp.resolve("input.csv");
        Files.write(input, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta",
                "3,Gamma").getBytes(StandardCharsets.UTF_8));
        Path output = tmp.resolve("output.csv");

        // maxRowsPerSplit=2 -> two splits: [1,2] and [3,3], so we exercise multi-split + checkpoints.
        PipelineConfig cfg = new PipelineConfig(
                "file-to-file",
                new PipelineConfig.SourceConfig("file", input.toString(), "csv", 2),
                new PipelineConfig.SinkConfig("file", output.toString(), "csv", "overwrite"),
                new PipelineConfig.CheckpointConfig(null));

        ConnectorFactory factory = new ConnectorFactory(SinkFactoryRegistry.defaultRegistry());

        List<SourceSplit> splits;
        SourceEnumerator src = factory.createSource(cfg);
        src.open();
        try {
            splits = src.enumerateSplits();
        } finally {
            src.close();
        }
        assertEquals(2, splits.size());
        long firstSeq = splits.get(0).getStartSeqInclusive();
        assertEquals(1L, firstSeq);

        TaskAssignment assignment = assignmentFor("task-success", cfg, splits, firstSeq);

        List<WorkerMessage> sent = new ArrayList<>();
        new WorkerTaskExecutor(
                "worker-1",
                factory,
                EmbeddingProviderRegistry.defaultRegistry(),
                sent::add)
                .execute(assignment);

        assertFalse(sent.isEmpty(), "expected at least one worker message");
        assertTrue(
                sent.stream().anyMatch(WorkerMessage::hasCheckpointAck),
                "expected at least one CheckpointAck");

        WorkerMessage last = sent.get(sent.size() - 1);
        assertTrue(last.hasTaskResult(), "last message must be a TaskResult");
        TaskAttemptResult result = last.getTaskResult();
        assertEquals(AttemptStatus.ATTEMPT_SUCCESS, result.getStatus());
        assertEquals("task-success", result.getTaskId());
        assertEquals("a1", result.getAttemptId());
        assertEquals("worker-1", result.getWorkerId());

        // CheckpointAcks should report processed seqs from the source's seq space (1..3).
        long maxAck = sent.stream()
                .filter(WorkerMessage::hasCheckpointAck)
                .mapToLong(m -> m.getCheckpointAck().getProcessedSeq())
                .max()
                .orElse(0L);
        assertEquals(3L, maxAck);

        assertTrue(Files.exists(output), "output file should exist");
        List<String> lines = Files.readAllLines(output, StandardCharsets.UTF_8);
        // CSV output = header line + one line per row.
        assertEquals(4, lines.size());
        assertEquals("id,content", lines.get(0));
    }

    @Test
    void unsupportedSinkTypeReportsAssemblyFailureWithoutThrowing() throws Exception {
        Path input = tmp.resolve("input.csv");
        Files.write(input, String.join("\n",
                "id,content",
                "1,Alpha").getBytes(StandardCharsets.UTF_8));

        PipelineConfig cfg = new PipelineConfig(
                "bad-sink",
                new PipelineConfig.SourceConfig("file", input.toString(), "csv"),
                new PipelineConfig.SinkConfig("nope", null, null, null),
                new PipelineConfig.CheckpointConfig(null));

        ConnectorFactory factory = new ConnectorFactory(SinkFactoryRegistry.defaultRegistry());
        SourceEnumerator src = factory.createSource(cfg);
        src.open();
        List<SourceSplit> splits;
        try {
            splits = src.enumerateSplits();
        } finally {
            src.close();
        }

        TaskAssignment assignment = assignmentFor("task-bad-sink", cfg, splits, 1L);

        List<WorkerMessage> sent = new ArrayList<>();
        // Must not throw out of execute() — failures are reported as messages.
        new WorkerTaskExecutor(
                "worker-1",
                factory,
                EmbeddingProviderRegistry.defaultRegistry(),
                sent::add)
                .execute(assignment);

        assertFalse(sent.isEmpty(), "expected a failure message");
        WorkerMessage last = sent.get(sent.size() - 1);
        assertTrue(last.hasTaskResult(), "last message must be a TaskResult");
        TaskAttemptResult result = last.getTaskResult();
        assertEquals(AttemptStatus.ATTEMPT_FAILED, result.getStatus());
        assertFalse(result.getErrorCode().isEmpty(), "errorCode must be non-empty");
    }

    @Test
    void missingPipelineConfigReportsAssemblyFailure() throws Exception {
        TaskDefinition def = new TaskDefinition();
        def.setTaskId("task-missing-pipeline");
        def.setJobName("job");
        Map<String, Object> config = new HashMap<>();
        config.put(JobSubmissionService.SPLITS_CONFIG_KEY, new ArrayList<>());
        // No PIPELINE_CONFIG_KEY entry.
        def.setConfig(config);

        TaskAssignment assignment = TaskAssignment.newBuilder()
                .setTaskId("task-missing-pipeline")
                .setAttemptId("a1")
                .setDefinition(serialize(def))
                .setStartSeq(1L)
                .setLeaseUntilMillis(System.currentTimeMillis() + 60_000L)
                .build();

        List<WorkerMessage> sent = new ArrayList<>();
        new WorkerTaskExecutor(
                "worker-1",
                new ConnectorFactory(SinkFactoryRegistry.defaultRegistry()),
                EmbeddingProviderRegistry.defaultRegistry(),
                sent::add)
                .execute(assignment);

        assertEquals(1, sent.size());
        WorkerMessage last = sent.get(0);
        assertTrue(last.hasTaskResult());
        TaskAttemptResult result = last.getTaskResult();
        assertEquals(AttemptStatus.ATTEMPT_FAILED, result.getStatus());
        assertEquals("ASSEMBLY", result.getErrorCode());
    }

    private TaskAssignment assignmentFor(
            String taskId, PipelineConfig cfg, List<SourceSplit> splits, long startSeq) throws Exception {
        TaskDefinition def = new TaskDefinition();
        def.setTaskId(taskId);
        def.setJobName("job-" + taskId);
        Map<String, Object> config = new HashMap<>();
        config.put(JobSubmissionService.SPLITS_CONFIG_KEY, new ArrayList<Object>(splits));
        config.put(WorkerTaskExecutor.PIPELINE_CONFIG_KEY, cfg);
        def.setConfig(config);

        return TaskAssignment.newBuilder()
                .setTaskId(taskId)
                .setAttemptId("a1")
                .setDefinition(serialize(def))
                .setStartSeq(startSeq)
                .setLeaseUntilMillis(System.currentTimeMillis() + 60_000L)
                .build();
    }

    private ByteString serialize(TaskDefinition def) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bytes)) {
            oos.writeObject(def);
        }
        return ByteString.copyFrom(bytes.toByteArray());
    }

}
