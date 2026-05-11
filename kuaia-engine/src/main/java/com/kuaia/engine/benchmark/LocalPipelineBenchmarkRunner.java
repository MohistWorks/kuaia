package com.kuaia.engine.benchmark;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.LocalPipelineRunner;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineRunSummary;
import com.kuaia.engine.pipeline.embedding.EmbeddingProvider;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import com.kuaia.engine.worker.connector.VectorSinkFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

public class LocalPipelineBenchmarkRunner {
    public static final int DEFAULT_ROWS = 128;
    public static final int DEFAULT_MAX_ROWS_PER_SPLIT = 0;
    public static final Path DEFAULT_OUTPUT = Paths.get(
            "target",
            "kuaia-benchmark",
            "local-pipeline-batch.json");

    private static final int[] BATCH_SIZES = new int[]{1, 8, 32, 128};

    public List<BenchmarkResult> run(BenchmarkOptions options, PrintStream out) throws Exception {
        Path output = options.getOutput();
        Path workDir = output.toAbsolutePath().getParent();
        if (workDir == null) {
            workDir = Paths.get("target", "kuaia-benchmark").toAbsolutePath();
            output = workDir.resolve(output);
        }
        Files.createDirectories(workDir);
        Path data = workDir.resolve("benchmark-documents.csv");
        writeCsv(data, options.getRows());

        List<BenchmarkResult> results = new ArrayList<>();
        out.println("Benchmark: local batch pipeline");
        out.println("rows=" + options.getRows()
                + " maxRowsPerSplit=" + displayMaxRowsPerSplit(options.getMaxRowsPerSplit())
                + " output=" + output);

        for (int batchSize : BATCH_SIZES) {
            CountingEmbeddingProvider provider = new CountingEmbeddingProvider();
            CountingVectorSink sink = new CountingVectorSink();
            LocalPipelineRunner runner = runner(provider, sink);
            Path stateDir = workDir.resolve("state-" + batchSize);
            deleteIfExists(stateDir);
            PipelineConfig config = benchmarkConfig(
                    data,
                    stateDir,
                    batchSize,
                    options.getMaxRowsPerSplit());

            long startedAt = System.nanoTime();
            PipelineRunSummary summary = runner.run(
                    config,
                    new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8.name()));
            long elapsedNanos = System.nanoTime() - startedAt;

            TaskRecord record;
            try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
                record = store.getTask("local-pipeline-benchmark-" + batchSize);
            }
            if (record == null || record.getState() != TaskState.COMPLETED) {
                throw new IllegalStateException("Benchmark task did not complete for batchSize=" + batchSize);
            }

            BenchmarkResult result = new BenchmarkResult(
                    options.getRows(),
                    batchSize,
                    elapsedNanos,
                    provider.batchCalls,
                    provider.singleCalls,
                    provider.rowsEmbedded,
                    sink.batchWrites,
                    sink.rowsWritten,
                    record.getVersion() - 3L,
                    summary.getRowsWritten(),
                    summary.getSourceSplits(),
                    summary.getSinkBatches());
            results.add(result);
            out.println(String.format(
                    Locale.ROOT,
                    "batchSize=%d rowsWritten=%d sourceSplits=%d sinkBatches=%d rowsPerSecond=%.3f",
                    batchSize,
                    result.getRowsWritten(),
                    result.getSourceSplits(),
                    result.getSinkBatches(),
                    result.rowsPerSecond()));
        }

        writeResults(output, results);
        return results;
    }

    private LocalPipelineRunner runner(CountingEmbeddingProvider provider, CountingVectorSink sink) {
        Map<String, EmbeddingProvider> providers = new HashMap<>();
        providers.put("counting", provider);
        SinkFactoryRegistry sinkFactories = new SinkFactoryRegistry(Collections.singletonMap(
                "mock-vector",
                (VectorSinkFactory) (rowType, out, sinkConfig) -> sink));
        return new LocalPipelineRunner(sinkFactories, new EmbeddingProviderRegistry(providers));
    }

    private PipelineConfig benchmarkConfig(Path data, Path stateDir, int batchSize, int maxRowsPerSplit) {
        return new PipelineConfig(
                "benchmark-" + batchSize,
                new PipelineConfig.SourceConfig("file", data.toString(), "csv", maxRowsPerSplit),
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

    private void writeCsv(Path data, int rowCount) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("id,content");
        for (int i = 1; i <= rowCount; i++) {
            lines.add(i + ",Document-" + i);
        }
        Files.write(data, lines, StandardCharsets.UTF_8);
    }

    private void writeResults(Path output, List<BenchmarkResult> results) throws IOException {
        Files.createDirectories(output.toAbsolutePath().getParent());
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

    private void deleteIfExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(path)) {
            paths.sorted(Comparator.reverseOrder()).forEach(current -> {
                try {
                    Files.deleteIfExists(current);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private String displayMaxRowsPerSplit(int maxRowsPerSplit) {
        return maxRowsPerSplit > 0 ? Integer.toString(maxRowsPerSplit) : "default";
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

    public static class BenchmarkOptions {
        private final int rows;
        private final int maxRowsPerSplit;
        private final Path output;

        public BenchmarkOptions(int rows, int maxRowsPerSplit, Path output) {
            if (rows <= 0) {
                throw new IllegalArgumentException("benchmark --rows must be greater than zero");
            }
            if (maxRowsPerSplit < 0) {
                throw new IllegalArgumentException("benchmark --max-rows-per-split must not be negative");
            }
            this.rows = rows;
            this.maxRowsPerSplit = maxRowsPerSplit;
            this.output = output;
        }

        public static BenchmarkOptions defaults() {
            return new BenchmarkOptions(DEFAULT_ROWS, DEFAULT_MAX_ROWS_PER_SPLIT, DEFAULT_OUTPUT);
        }

        public int getRows() {
            return rows;
        }

        public int getMaxRowsPerSplit() {
            return maxRowsPerSplit;
        }

        public Path getOutput() {
            return output;
        }
    }

    public static class BenchmarkResult {
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

        public int getRowsWritten() {
            return rowsWritten;
        }

        public long getSourceSplits() {
            return sourceSplits;
        }

        public long getSinkBatches() {
            return sinkBatches;
        }

        public double rowsPerSecond() {
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
