package com.kuaia.engine.benchmark;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.LocalPipelineRunner;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.pipeline.PipelineRunSummary;
import com.kuaia.engine.pipeline.embedding.EmbeddingProvider;
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalPipelineBenchmarkTest {
    private static final int[] BATCH_SIZES = new int[]{1, 8, 32, 128};

    @TempDir
    Path tempDir;

    @Test
    void measuresLocalBatchPipelineCounters() throws Exception {
        int rowCount = Integer.getInteger("kuaia.benchmark.rows", 128);
        Path data = tempDir.resolve("benchmark-documents.csv");
        writeCsv(data, rowCount);
        List<BenchmarkResult> results = new ArrayList<>();

        for (int batchSize : BATCH_SIZES) {
            CountingEmbeddingProvider provider = new CountingEmbeddingProvider();
            CountingVectorSink sink = new CountingVectorSink();
            Map<String, EmbeddingProvider> providers = new HashMap<>();
            providers.put("counting", provider);
            SinkFactoryRegistry sinkFactories = new SinkFactoryRegistry(Collections.singletonMap(
                    "mock-vector",
                    (VectorSinkFactory) (rowType, out, sinkConfig) -> sink));
            LocalPipelineRunner runner = new LocalPipelineRunner(
                    sinkFactories,
                    new EmbeddingProviderRegistry(providers));
            Path stateDir = tempDir.resolve("state-" + batchSize);
            PipelineConfig config = benchmarkConfig(data, stateDir, batchSize);

            long startedAt = System.nanoTime();
            PipelineRunSummary summary = runner.run(
                    config,
                    new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8.name()));
            long elapsedNanos = System.nanoTime() - startedAt;

            TaskRecord record;
            try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
                record = store.getTask("local-pipeline-benchmark-" + batchSize);
            }
            assertNotNull(record);

            int expectedBatches = expectedBatches(rowCount, batchSize);
            long checkpointUpdates = record.getVersion() - 3L;
            BenchmarkResult result = new BenchmarkResult(
                    rowCount,
                    batchSize,
                    elapsedNanos,
                    provider.batchCalls,
                    provider.singleCalls,
                    provider.rowsEmbedded,
                    sink.batchWrites,
                    sink.rowsWritten,
                    checkpointUpdates,
                    summary.getRowsWritten(),
                    summary.getSourceSplits(),
                    summary.getSinkBatches());
            results.add(result);

            assertEquals(TaskState.COMPLETED, record.getState());
            assertEquals(rowCount, record.getLastCheckpointSeq());
            assertEquals(expectedBatches, provider.batchCalls);
            assertEquals(0, provider.singleCalls);
            assertEquals(rowCount, provider.rowsEmbedded);
            assertEquals(expectedBatches, sink.batchWrites);
            assertEquals(rowCount, sink.rowsWritten);
            assertEquals(expectedBatches, checkpointUpdates);
            assertEquals(rowCount, summary.getRowsWritten());
            assertEquals(1L, summary.getSourceSplits());
            assertEquals(expectedBatches, summary.getSinkBatches());
            assertTrue(result.rowsPerSecond() > 0.0d);
        }

        Path output = Paths.get("target", "kuaia-benchmark", "local-pipeline-batch.json");
        writeResults(output, results);
        assertTrue(Files.exists(output));
        String json = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"batchSize\":32"));
        assertTrue(json.contains("\"sourceSplits\":1"));
        assertTrue(json.contains("\"sinkBatches\":4"));
    }

    private PipelineConfig benchmarkConfig(Path data, Path stateDir, int batchSize) {
        return new PipelineConfig(
                "benchmark-" + batchSize,
                new PipelineConfig.SourceConfig("file", data.toString(), "csv"),
                Collections.singletonList(new PipelineConfig.TransformConfig(
                        "embedding",
                        Collections.emptyList(),
                        null,
                        null,
                        "content",
                        "embedding",
                        4,
                        "counting",
                        null,
                        null,
                        null,
                        30000,
                        batchSize)),
                new PipelineConfig.SinkConfig("mock-vector"),
                new PipelineConfig.CheckpointConfig(stateDir.toString()));
    }

    private void writeCsv(Path data, int rowCount) throws Exception {
        List<String> lines = new ArrayList<>();
        lines.add("id,content");
        for (int i = 1; i <= rowCount; i++) {
            lines.add(i + ",Document-" + i);
        }
        Files.write(data, lines, StandardCharsets.UTF_8);
    }

    private int expectedBatches(int rowCount, int batchSize) {
        return (rowCount + batchSize - 1) / batchSize;
    }

    private void writeResults(Path output, List<BenchmarkResult> results) throws Exception {
        Files.createDirectories(output.getParent());
        StringBuilder json = new StringBuilder();
        json.append("[\n");
        for (int i = 0; i < results.size(); i++) {
            if (i > 0) {
                json.append(",\n");
            }
            json.append(results.get(i).toJson());
        }
        json.append("\n]\n");
        Files.write(output, json.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static class CountingEmbeddingProvider implements EmbeddingProvider {
        private int singleCalls;
        private int batchCalls;
        private int rowsEmbedded;

        @Override
        public float[] embed(String input, int dimensions) {
            singleCalls++;
            rowsEmbedded++;
            return vector(input, dimensions);
        }

        @Override
        public List<float[]> embedBatch(List<String> inputs, int dimensions) {
            batchCalls++;
            rowsEmbedded += inputs.size();
            List<float[]> vectors = new ArrayList<>();
            for (String input : inputs) {
                vectors.add(vector(input, dimensions));
            }
            return vectors;
        }

        private float[] vector(String input, int dimensions) {
            float[] vector = new float[dimensions];
            for (int i = 0; i < dimensions; i++) {
                vector[i] = input.length() + i;
            }
            return vector;
        }
    }

    private static class CountingVectorSink implements SinkWriter {
        private int batchWrites;
        private int rowsWritten;

        @Override
        public void open() {}

        @Override
        public void write(BinaryRow row) {
            throw new AssertionError("benchmark sink expects batch writes");
        }

        @Override
        public void writeBatch(List<BinaryRow> rows) {
            batchWrites++;
            rowsWritten += rows.size();
        }

        @Override
        public void close() {}
    }

    private static class BenchmarkResult {
        private final int rowCount;
        private final int batchSize;
        private final long elapsedNanos;
        private final int embeddingBatchCalls;
        private final int embeddingSingleCalls;
        private final int rowsEmbedded;
        private final int sinkBatchWrites;
        private final int rowsWritten;
        private final long checkpointUpdates;
        private final long summaryRowsWritten;
        private final long sourceSplits;
        private final long sinkBatches;

        private BenchmarkResult(
                int rowCount,
                int batchSize,
                long elapsedNanos,
                int embeddingBatchCalls,
                int embeddingSingleCalls,
                int rowsEmbedded,
                int sinkBatchWrites,
                int rowsWritten,
                long checkpointUpdates,
                long summaryRowsWritten,
                long sourceSplits,
                long sinkBatches) {
            this.rowCount = rowCount;
            this.batchSize = batchSize;
            this.elapsedNanos = elapsedNanos;
            this.embeddingBatchCalls = embeddingBatchCalls;
            this.embeddingSingleCalls = embeddingSingleCalls;
            this.rowsEmbedded = rowsEmbedded;
            this.sinkBatchWrites = sinkBatchWrites;
            this.rowsWritten = rowsWritten;
            this.checkpointUpdates = checkpointUpdates;
            this.summaryRowsWritten = summaryRowsWritten;
            this.sourceSplits = sourceSplits;
            this.sinkBatches = sinkBatches;
        }

        private double rowsPerSecond() {
            return rowCount / Math.max(elapsedNanos / 1_000_000_000.0d, 0.000001d);
        }

        private String toJson() {
            return String.format(Locale.ROOT,
                    "  {\"rowCount\":%d,\"batchSize\":%d,\"durationMs\":%.3f,"
                            + "\"rowsPerSecond\":%.3f,\"embeddingBatchCalls\":%d,"
                            + "\"embeddingSingleCalls\":%d,\"rowsEmbedded\":%d,"
                            + "\"sinkBatchWrites\":%d,\"rowsWritten\":%d,"
                            + "\"checkpointUpdates\":%d,\"summaryRowsWritten\":%d,"
                            + "\"sourceSplits\":%d,\"sinkBatches\":%d}",
                    rowCount,
                    batchSize,
                    elapsedNanos / 1_000_000.0d,
                    rowsPerSecond(),
                    embeddingBatchCalls,
                    embeddingSingleCalls,
                    rowsEmbedded,
                    sinkBatchWrites,
                    rowsWritten,
                    checkpointUpdates,
                    summaryRowsWritten,
                    sourceSplits,
                    sinkBatches);
        }
    }
}
