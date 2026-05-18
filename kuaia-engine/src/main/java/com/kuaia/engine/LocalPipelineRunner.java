package com.kuaia.engine;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.pipeline.LocalPipelineCheckpointStore;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.pipeline.PipelineRunSummary;
import com.kuaia.engine.worker.connector.ConsoleSink;
import com.kuaia.engine.worker.connector.FakeSource;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import com.kuaia.engine.pipeline.transform.TransformPipeline;
import com.kuaia.engine.worker.connector.FileSource;
import com.kuaia.engine.worker.connector.FileSink;
import com.kuaia.engine.worker.connector.DuckDBSource;
import com.kuaia.engine.worker.connector.LocalSource;
import com.kuaia.engine.worker.connector.MySQLSource;
import com.kuaia.engine.worker.connector.PostgresSource;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import com.kuaia.engine.worker.connector.v2.BatchCommit;
import com.kuaia.engine.worker.connector.v2.BatchSinkWriter;
import com.kuaia.engine.worker.connector.v2.BatchSourceReader;
import com.kuaia.engine.worker.connector.v2.FileSourceAdapter;
import com.kuaia.engine.worker.connector.v2.LocalSourceAdapter;
import com.kuaia.engine.worker.connector.v2.SinkWriterBatchAdapter;
import com.kuaia.engine.worker.connector.v2.SourceEnumerator;
import com.kuaia.engine.worker.connector.v2.SourceSplit;

import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class LocalPipelineRunner {
    private static final int DEFAULT_FILE_ROWS_PER_SPLIT = 10_000;
    private static final String SOURCE_STAGE = "Source";
    private static final String TRANSFORM_STAGE = "Transform";
    private static final String SINK_STAGE = "Sink";
    private static final String CHECKPOINT_STAGE = "Checkpoint";

    private final SinkFactoryRegistry sinkFactories;
    private final EmbeddingProviderRegistry embeddingProviders;
    private final int fileRowsPerSplit;

    public LocalPipelineRunner() {
        this(SinkFactoryRegistry.defaultRegistry());
    }

    public LocalPipelineRunner(SinkFactoryRegistry sinkFactories) {
        this(sinkFactories, EmbeddingProviderRegistry.defaultRegistry());
    }

    public LocalPipelineRunner(SinkFactoryRegistry sinkFactories, EmbeddingProviderRegistry embeddingProviders) {
        this(sinkFactories, embeddingProviders, DEFAULT_FILE_ROWS_PER_SPLIT);
    }

    LocalPipelineRunner(
            SinkFactoryRegistry sinkFactories,
            EmbeddingProviderRegistry embeddingProviders,
            int fileRowsPerSplit) {
        if (fileRowsPerSplit <= 0) {
            throw new IllegalArgumentException("fileRowsPerSplit must be greater than zero");
        }
        this.sinkFactories = sinkFactories;
        this.embeddingProviders = embeddingProviders;
        this.fileRowsPerSplit = fileRowsPerSplit;
    }

    public int run(PrintStream out) throws Exception {
        FakeSource source = new FakeSource();
        ConsoleSink sink = new ConsoleSink(source.getRowType(), out);

        source.open();
        sink.open();
        int rows = 0;
        try {
            out.println("Starting Local Pipeline...");
            for (int i = 0; i < 10; i++) {
                source.pollNext(sink::write);
                rows++;
            }
            out.println("Pipeline Finished. rows=" + rows);
            return rows;
        } finally {
            source.close();
            sink.close();
        }
    }

    public PipelineRunSummary run(PipelineConfig config, PrintStream out) throws Exception {
        long startedAt = System.nanoTime();
        PipelineRunSummary summary;
        if (hasCheckpointStateDir(config)) {
            summary = runWithCheckpoint(config, out);
        } else {
            summary = runWithoutCheckpoint(config, out);
        }
        long durationMillis = (System.nanoTime() - startedAt) / 1_000_000L;
        return summary.withDurationMillis(durationMillis);
    }

    private PipelineRunSummary runWithoutCheckpoint(PipelineConfig config, PrintStream out) throws Exception {
        SourceEnumerator source = runStage(SOURCE_STAGE, () -> createSource(config));
        BatchSinkWriter sink = null;
        try {
            runStage(SOURCE_STAGE, () -> {
                source.open();
                return null;
            });
            TransformPipeline transforms = runStage(TRANSFORM_STAGE, () -> TransformPipeline.from(
                    source.getRowType(),
                    config.getTransforms(),
                    embeddingProviders));
            sink = runStage(SINK_STAGE, () -> createSink(config, transforms.getOutputType(), out));
            BatchSinkWriter openedSink = sink;
            runStage(SINK_STAGE, () -> {
                openedSink.open();
                return null;
            });
            out.println("Starting pipeline: " + config.getName());
            PipelineCounters counters = new PipelineCounters();
            BatchBuffer batch = new BatchBuffer(transforms.getBatchSize());
            for (SourceSplit split : runStage(SOURCE_STAGE, source::enumerateSplits)) {
                counters.recordSourceSplit();
                BatchSourceReader reader = runStage(SOURCE_STAGE, () -> source.createReader(split));
                runStage(SOURCE_STAGE, () -> {
                    reader.readFrom(
                            0L,
                            (seqId, row) -> {
                                batch.add(seqId, row);
                                if (batch.isFull()) {
                                    flushBatch(split, batch, transforms, openedSink, counters, null);
                                }
                            },
                            (seqId, error) -> {
                                flushBatch(split, batch, transforms, openedSink, counters, null);
                                return handleRecordError(config, out, counters, seqId, error);
                            });
                    return null;
                });
                flushBatch(split, batch, transforms, openedSink, counters, null);
            }
            out.println("Pipeline Finished. rows=" + counters.rowsWritten);
            return counters.toSummary(TaskState.COMPLETED);
        } finally {
            source.close();
            if (sink != null) {
                sink.close();
            }
        }
    }

    private PipelineRunSummary runWithCheckpoint(PipelineConfig config, PrintStream out) throws Exception {
        SourceEnumerator source = runStage(SOURCE_STAGE, () -> createSource(config));
        BatchSinkWriter sink = null;
        try {
            runStage(SOURCE_STAGE, () -> {
                source.open();
                return null;
            });
            TransformPipeline transforms = runStage(TRANSFORM_STAGE, () -> TransformPipeline.from(
                    source.getRowType(),
                    config.getTransforms(),
                    embeddingProviders));
            out.println("Starting pipeline: " + config.getName());
            try (LocalPipelineCheckpointStore checkpointStore = runStage(
                    CHECKPOINT_STAGE,
                    () -> new LocalPipelineCheckpointStore(
                            Paths.get(config.getCheckpoint().getStateDir()),
                            config.getName()))) {
                TaskRecord task = runStage(CHECKPOINT_STAGE, checkpointStore::startOrResume);
                if (task.getState() == TaskState.COMPLETED) {
                    out.println("Pipeline Finished. rows=0 checkpoint="
                            + task.getLastCheckpointSeq()
                            + " state="
                            + task.getState());
                    return new PipelineRunSummary(
                            0L,
                            0L,
                            0L,
                            task.getLastCheckpointSeq(),
                            task.getLastCheckpointSeq(),
                            task.getState(),
                            0L);
                }

                sink = runStage(SINK_STAGE, () -> createSink(config, transforms.getOutputType(), out));
                BatchSinkWriter openedSink = sink;
                runStage(SINK_STAGE, () -> {
                    openedSink.open();
                    return null;
                });
                final TaskRecord[] taskRef = new TaskRecord[]{task};
                PipelineCounters counters = new PipelineCounters();
                BatchBuffer batch = new BatchBuffer(transforms.getBatchSize());
                counters.rowsSkipped = task.getLastCheckpointSeq();
                counters.checkpointSeq = task.getLastCheckpointSeq();
                for (SourceSplit split : runStage(SOURCE_STAGE, source::enumerateSplits)) {
                    counters.recordSourceSplit();
                    BatchSourceReader reader = runStage(SOURCE_STAGE, () -> source.createReader(split));
                    runStage(SOURCE_STAGE, () -> {
                        reader.readFrom(
                                task.getLastCheckpointSeq(),
                                (seqId, row) -> {
                                    batch.add(seqId, row);
                                    if (batch.isFull()) {
                                        flushBatch(
                                                split,
                                                batch,
                                                transforms,
                                                openedSink,
                                                counters,
                                                maxCommittedSeqId -> taskRef[0] = checkpointStore.checkpointBatch(
                                                        taskRef[0],
                                                        maxCommittedSeqId));
                                    }
                                },
                                (seqId, error) -> {
                                    flushBatch(
                                            split,
                                            batch,
                                            transforms,
                                            openedSink,
                                            counters,
                                            maxCommittedSeqId -> taskRef[0] = checkpointStore.checkpointBatch(
                                                    taskRef[0],
                                                    maxCommittedSeqId));
                                    boolean skipped = handleRecordError(config, out, counters, seqId, error);
                                    if (skipped) {
                                        taskRef[0] = checkpointStore.checkpoint(taskRef[0], seqId);
                                    }
                                    return skipped;
                                });
                        return null;
                    });
                    flushBatch(
                            split,
                            batch,
                            transforms,
                            openedSink,
                            counters,
                            maxCommittedSeqId -> taskRef[0] = checkpointStore.checkpointBatch(
                                    taskRef[0],
                                    maxCommittedSeqId));
                }
                TaskRecord completed = runStage(CHECKPOINT_STAGE, () -> checkpointStore.complete(taskRef[0]));
                out.println("Pipeline Finished. rows="
                        + counters.rowsWritten
                        + " checkpoint="
                        + completed.getLastCheckpointSeq()
                        + " state="
                        + completed.getState());
                counters.checkpointSeq = completed.getLastCheckpointSeq();
                return counters.toSummary(completed.getState());
            }
        } finally {
            source.close();
            if (sink != null) {
                sink.close();
            }
        }
    }

    private SourceEnumerator createSource(PipelineConfig config) throws PipelineExecutionException {
        String sourceType = config.getSource().getType();
        if ("file".equals(sourceType)) {
            return new FileSourceAdapter(
                    new FileSource(Paths.get(config.getSource().getPath()), config.getSource().getFormat()),
                    "file-0",
                    fileRowsPerSplit(config));
        }
        if ("postgres".equals(sourceType)) {
            return new LocalSourceAdapter(new PostgresSource(config.getSource()), "postgres-0");
        }
        if ("mysql".equals(sourceType)) {
            return new LocalSourceAdapter(new MySQLSource(config.getSource()), "mysql-0");
        }
        if ("duckdb".equals(sourceType)) {
            return new LocalSourceAdapter(new DuckDBSource(config.getSource()), "duckdb-0");
        }
        throw new PipelineExecutionException("Unsupported source.type: " + sourceType);
    }

    private int fileRowsPerSplit(PipelineConfig config) {
        int configured = config.getSource().getMaxRowsPerSplit();
        return configured > 0 ? configured : fileRowsPerSplit;
    }

    private boolean hasCheckpointStateDir(PipelineConfig config) {
        String stateDir = config.getCheckpoint().getStateDir();
        return stateDir != null && !stateDir.trim().isEmpty();
    }

    private BatchSinkWriter createSink(PipelineConfig config, KuaiaRowType rowType, PrintStream out)
            throws PipelineExecutionException {
        String sinkType = config.getSink().getType();
        SinkWriter sink;
        if ("console".equals(sinkType)) {
            sink = new ConsoleSink(rowType, out);
        } else if ("mock-vector".equals(sinkType) || "qdrant".equals(sinkType)) {
            sink = sinkFactories.create(sinkType, rowType, out, config.getSink());
        } else if ("file".equals(sinkType)) {
            sink = new FileSink(
                    rowType,
                    Paths.get(config.getSink().getPath()),
                    config.getSink().getFormat(),
                    config.getSink().getMode());
        } else {
            throw new PipelineExecutionException("Unsupported sink.type: " + sinkType);
        }
        return new SinkWriterBatchAdapter(sink);
    }

    private boolean handleRecordError(
            PipelineConfig config,
            PrintStream out,
            PipelineCounters counters,
            long seqId,
            PipelineExecutionException error) {
        if (!config.getErrorPolicy().shouldSkipBadRecords()) {
            return false;
        }
        counters.recordFailed(seqId);
        out.println("Skipped bad record seq=" + seqId + " error=" + error.getMessage());
        return true;
    }

    private void flushBatch(
            SourceSplit split,
            BatchBuffer batch,
            TransformPipeline transforms,
            BatchSinkWriter sink,
            PipelineCounters counters,
            BatchSeqIdCommitter committer) throws Exception {
        if (batch.isEmpty()) {
            return;
        }
        List<Long> seqIds = batch.seqIds();
        List<BinaryRow> outputs = runStage(TRANSFORM_STAGE, () -> transforms.applyBatch(batch.rows()));
        long maxSeqId = maxSeqId(seqIds);
        if (!outputs.isEmpty()) {
            runStage(SINK_STAGE, () -> {
                sink.writeBatch(outputs);
                sink.committer().commit(new BatchCommit(split.getSplitId(), maxSeqId, outputs.size()));
                return null;
            });
            counters.recordSinkBatch();
        }
        if (committer != null) {
            runStage(CHECKPOINT_STAGE, () -> {
                committer.commit(maxSeqId);
                return null;
            });
        }
        counters.recordWrittenBatch(seqIds.size(), outputs.size(), maxSeqId);
        batch.clear();
    }

    private long maxSeqId(List<Long> seqIds) {
        long max = 0L;
        for (Long seqId : seqIds) {
            if (seqId > max) {
                max = seqId;
            }
        }
        return max;
    }

    private <T> T runStage(String stage, StageOperation<T> operation) throws Exception {
        try {
            return operation.run();
        } catch (PipelineExecutionException e) {
            if (isStageFailure(e)) {
                throw e;
            }
            throw new PipelineExecutionException(stage + " stage failed: " + e.getMessage(), e);
        } catch (Exception e) {
            if (isStageFailure(e)) {
                throw e;
            }
            throw new PipelineExecutionException(stage + " stage failed: " + errorMessage(e), e);
        }
    }

    private boolean isStageFailure(Exception error) {
        String message = error.getMessage();
        return message != null
                && (message.startsWith(SOURCE_STAGE + " stage failed:")
                || message.startsWith(TRANSFORM_STAGE + " stage failed:")
                || message.startsWith(SINK_STAGE + " stage failed:")
                || message.startsWith(CHECKPOINT_STAGE + " stage failed:"));
    }

    private String errorMessage(Exception error) {
        String message = error.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return error.getClass().getSimpleName();
        }
        return message;
    }

    private interface StageOperation<T> {
        T run() throws Exception;
    }

    private interface BatchSeqIdCommitter {
        void commit(long maxSeqId) throws Exception;
    }

    private static class BatchBuffer {
        private final int batchSize;
        private final List<Long> seqIds = new ArrayList<>();
        private final List<com.kuaia.common.data.BinaryRow> rows = new ArrayList<>();

        private BatchBuffer(int batchSize) {
            this.batchSize = Math.max(1, batchSize);
        }

        private void add(long seqId, com.kuaia.common.data.BinaryRow row) {
            seqIds.add(seqId);
            rows.add(row);
        }

        private boolean isFull() {
            return rows.size() >= batchSize;
        }

        private boolean isEmpty() {
            return rows.isEmpty();
        }

        private List<Long> seqIds() {
            return new ArrayList<>(seqIds);
        }

        private List<com.kuaia.common.data.BinaryRow> rows() {
            return new ArrayList<>(rows);
        }

        private void clear() {
            seqIds.clear();
            rows.clear();
        }
    }

    private static class PipelineCounters {
        private long rowsRead;
        private long rowsWritten;
        private long rowsFailed;
        private long rowsSkipped;
        private long checkpointSeq;
        private long sourceSplits;
        private long sinkBatches;

        private void recordSourceSplit() {
            sourceSplits++;
        }

        private void recordSinkBatch() {
            sinkBatches++;
        }

        private void recordWrittenBatch(int sourceRows, int outputRows, long maxSeqId) {
            rowsRead += sourceRows;
            rowsWritten += outputRows;
            checkpointSeq = maxSeqId;
        }

        private void recordFailed(long seqId) {
            rowsRead++;
            rowsFailed++;
            checkpointSeq = seqId;
        }

        private PipelineRunSummary toSummary(TaskState taskState) {
            return new PipelineRunSummary(
                    rowsRead,
                    rowsWritten,
                    rowsFailed,
                    rowsSkipped,
                    checkpointSeq,
                    taskState,
                    sourceSplits,
                    sinkBatches,
                    0L);
        }
    }
}
