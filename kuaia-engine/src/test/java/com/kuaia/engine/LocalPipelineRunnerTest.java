package com.kuaia.engine;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineConfigLoader;
import com.kuaia.engine.pipeline.PipelineRunSummary;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import com.kuaia.engine.worker.connector.VectorSinkFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalPipelineRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void writesVectorPipelineRowsInConfiguredBatches() throws Exception {
        Path data = tempDir.resolve("documents.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta",
                "3,Gamma").getBytes(StandardCharsets.UTF_8));
        Path configPath = tempDir.resolve("batched-vector.yaml");
        Files.write(configPath, String.join("\n",
                "name: batched-vector",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "    dimensions: 4",
                "    batchSize: 2",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));
        CapturingSink sink = new CapturingSink();
        PipelineConfig config = new PipelineConfigLoader().load(configPath);
        SinkFactoryRegistry registry = new SinkFactoryRegistry(Collections.singletonMap(
                "mock-vector",
                (VectorSinkFactory) (rowType, out, sinkConfig) -> sink));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new LocalPipelineRunner(registry).run(config, new PrintStream(bytes, true, StandardCharsets.UTF_8.name()));

        assertEquals(0, sink.singleWrites);
        assertEquals(2, sink.batchWrites);
        assertEquals(java.util.Arrays.asList(2, 1), sink.batchSizes);
    }

    @Test
    void checkpointsOncePerSuccessfulSinkBatch() throws Exception {
        Path data = tempDir.resolve("checkpointed-documents.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta",
                "3,Gamma").getBytes(StandardCharsets.UTF_8));
        Path stateDir = tempDir.resolve("checkpointed-batch-state");
        Path configPath = tempDir.resolve("checkpointed-batch-vector.yaml");
        Files.write(configPath, String.join("\n",
                "name: checkpointed-batch-vector",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "    dimensions: 4",
                "    batchSize: 2",
                "sink:",
                "  type: mock-vector",
                "checkpoint:",
                "  stateDir: " + stateDir).getBytes(StandardCharsets.UTF_8));
        CapturingSink sink = new CapturingSink();
        PipelineConfig config = new PipelineConfigLoader().load(configPath);
        SinkFactoryRegistry registry = new SinkFactoryRegistry(Collections.singletonMap(
                "mock-vector",
                (VectorSinkFactory) (rowType, out, sinkConfig) -> sink));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new LocalPipelineRunner(registry).run(config, new PrintStream(bytes, true, StandardCharsets.UTF_8.name()));

        assertEquals(java.util.Arrays.asList(2, 1), sink.batchSizes);
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-checkpointed-batch-vector");
            assertNotNull(record);
            assertEquals(TaskState.COMPLETED, record.getState());
            assertEquals(3L, record.getLastCheckpointSeq());
            assertEquals(5L, record.getVersion());
        }
    }

    @Test
    void processesFileSourceAcrossMultipleInternalSplits() throws Exception {
        Path data = tempDir.resolve("split-documents.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta",
                "3,Gamma",
                "4,Delta",
                "5,Epsilon").getBytes(StandardCharsets.UTF_8));
        Path configPath = tempDir.resolve("split-vector.yaml");
        Files.write(configPath, String.join("\n",
                "name: split-vector",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "    dimensions: 4",
                "    batchSize: 10",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));
        CapturingSink sink = new CapturingSink();
        PipelineConfig config = new PipelineConfigLoader().load(configPath);
        SinkFactoryRegistry registry = new SinkFactoryRegistry(Collections.singletonMap(
                "mock-vector",
                (VectorSinkFactory) (rowType, out, sinkConfig) -> sink));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PipelineRunSummary summary = new LocalPipelineRunner(registry, EmbeddingProviderRegistry.defaultRegistry(), 2)
                .run(config, new PrintStream(bytes, true, StandardCharsets.UTF_8.name()));

        assertEquals(0, sink.singleWrites);
        assertEquals(java.util.Arrays.asList(2, 2, 1), sink.batchSizes);
        assertEquals(3L, summary.getSourceSplits());
        assertEquals(3L, summary.getSinkBatches());
    }

    @Test
    void rejectsNonPositiveFileRowsPerSplit() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new LocalPipelineRunner(
                        SinkFactoryRegistry.defaultRegistry(),
                        EmbeddingProviderRegistry.defaultRegistry(),
                        0));

        assertEquals("fileRowsPerSplit must be greater than zero", error.getMessage());
    }

    @Test
    void resumesFileSourceAfterFailedSplitBatch() throws Exception {
        Path data = tempDir.resolve("resume-split-documents.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta",
                "3,Gamma",
                "4,Delta",
                "5,Epsilon").getBytes(StandardCharsets.UTF_8));
        Path stateDir = tempDir.resolve("resume-split-state");
        Path configPath = tempDir.resolve("resume-split-vector.yaml");
        Files.write(configPath, String.join("\n",
                "name: resume-split-vector",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "    dimensions: 4",
                "    batchSize: 2",
                "sink:",
                "  type: mock-vector",
                "checkpoint:",
                "  stateDir: " + stateDir).getBytes(StandardCharsets.UTF_8));
        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        FailingOnBatchSink firstSink = new FailingOnBatchSink(2);
        SinkFactoryRegistry firstRegistry = new SinkFactoryRegistry(Collections.singletonMap(
                "mock-vector",
                (VectorSinkFactory) (rowType, out, sinkConfig) -> firstSink));
        ByteArrayOutputStream firstBytes = new ByteArrayOutputStream();
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> new LocalPipelineRunner(
                        firstRegistry,
                        EmbeddingProviderRegistry.defaultRegistry(),
                        2)
                        .run(config, new PrintStream(firstBytes, true, StandardCharsets.UTF_8.name())));

        assertEquals("sink batch failed", error.getMessage());
        assertEquals(java.util.Arrays.asList(2, 2), firstSink.batchSizes());
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-resume-split-vector");
            assertNotNull(record);
            assertEquals(TaskState.RUNNING, record.getState());
            assertEquals(2L, record.getLastCheckpointSeq());
        }

        CapturingSink resumedSink = new CapturingSink();
        SinkFactoryRegistry resumedRegistry = new SinkFactoryRegistry(Collections.singletonMap(
                "mock-vector",
                (VectorSinkFactory) (rowType, out, sinkConfig) -> resumedSink));
        ByteArrayOutputStream resumedBytes = new ByteArrayOutputStream();
        PipelineRunSummary summary = new LocalPipelineRunner(
                resumedRegistry,
                EmbeddingProviderRegistry.defaultRegistry(),
                2)
                .run(config, new PrintStream(resumedBytes, true, StandardCharsets.UTF_8.name()));

        assertEquals(java.util.Arrays.asList(2, 1), resumedSink.batchSizes);
        assertEquals(3L, summary.getRowsWritten());
        assertEquals(2L, summary.getRowsSkipped());
        assertEquals(5L, summary.getCheckpointSeq());
        assertEquals(TaskState.COMPLETED, summary.getTaskState());
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-resume-split-vector");
            assertNotNull(record);
            assertEquals(TaskState.COMPLETED, record.getState());
            assertEquals(5L, record.getLastCheckpointSeq());
        }
    }

    @Test
    void doesNotCheckpointFailedSinkBatch() throws Exception {
        Path data = tempDir.resolve("failed-batch-documents.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta").getBytes(StandardCharsets.UTF_8));
        Path stateDir = tempDir.resolve("failed-batch-state");
        Path configPath = tempDir.resolve("failed-batch-vector.yaml");
        Files.write(configPath, String.join("\n",
                "name: failed-batch-vector",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "    dimensions: 4",
                "    batchSize: 2",
                "sink:",
                "  type: mock-vector",
                "checkpoint:",
                "  stateDir: " + stateDir).getBytes(StandardCharsets.UTF_8));
        FailingSink sink = new FailingSink();
        PipelineConfig config = new PipelineConfigLoader().load(configPath);
        SinkFactoryRegistry registry = new SinkFactoryRegistry(Collections.singletonMap(
                "mock-vector",
                (VectorSinkFactory) (rowType, out, sinkConfig) -> sink));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> new LocalPipelineRunner(registry)
                        .run(config, new PrintStream(bytes, true, StandardCharsets.UTF_8.name())));

        assertEquals("sink batch failed", error.getMessage());
        assertEquals(1, sink.batchWrites());
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-failed-batch-vector");
            assertNotNull(record);
            assertEquals(TaskState.RUNNING, record.getState());
            assertEquals(0L, record.getLastCheckpointSeq());
            assertEquals(2L, record.getVersion());
        }
    }

    private static class CapturingSink implements SinkWriter {
        private int singleWrites;
        private int batchWrites;
        private final List<Integer> batchSizes = new ArrayList<>();

        @Override
        public void open() {}

        @Override
        public void write(BinaryRow row) {
            singleWrites++;
        }

        @Override
        public void writeBatch(List<BinaryRow> rows) {
            batchWrites++;
            batchSizes.add(rows.size());
        }

        int batchWrites() {
            return batchWrites;
        }

        List<Integer> batchSizes() {
            return batchSizes;
        }

        @Override
        public void close() {}
    }

    private static class FailingSink extends CapturingSink {
        @Override
        public void writeBatch(List<BinaryRow> rows) {
            super.writeBatch(rows);
            throw new IllegalStateException("sink batch failed");
        }
    }

    private static class FailingOnBatchSink extends CapturingSink {
        private final int failedBatch;

        private FailingOnBatchSink(int failedBatch) {
            this.failedBatch = failedBatch;
        }

        @Override
        public void writeBatch(List<BinaryRow> rows) {
            super.writeBatch(rows);
            if (batchWrites() == failedBatch) {
                throw new IllegalStateException("sink batch failed");
            }
        }
    }
}
