package com.kuaia.engine.pipeline;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.engine.pipeline.transform.TransformPipeline;
import com.kuaia.engine.worker.connector.v2.BatchCommit;
import com.kuaia.engine.worker.connector.v2.BatchSinkWriter;
import com.kuaia.engine.worker.connector.v2.BatchSourceReader;
import com.kuaia.engine.worker.connector.v2.SourceEnumerator;
import com.kuaia.engine.worker.connector.v2.SourceSplit;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs the read/transform/write/checkpoint loop for a single {@link SourceSplit}.
 *
 * <p>The caller is responsible for opening {@code source} and {@code sink} before invoking
 * {@link #execute} and for closing them afterwards. This class only drives the per-split
 * processing so the local runner and a future worker can share the same logic.
 */
public final class SplitExecutor {
    private static final String SOURCE_STAGE = "Source";
    private static final String TRANSFORM_STAGE = "Transform";
    private static final String SINK_STAGE = "Sink";
    private static final String CHECKPOINT_STAGE = "Checkpoint";

    public SplitResult execute(
            SourceEnumerator source,
            TransformPipeline transforms,
            BatchSinkWriter sink,
            SourceSplit split,
            long lastCheckpointSeq,
            PipelineConfig.ErrorPolicyConfig errorPolicy,
            PrintStream out,
            CheckpointCallback checkpoint) throws Exception {
        BatchBuffer batch = new BatchBuffer(transforms.getBatchSize());
        SplitCounters counters = new SplitCounters();
        BatchSourceReader reader = runStage(SOURCE_STAGE, () -> source.createReader(split));
        runStage(SOURCE_STAGE, () -> {
            reader.readFrom(
                    lastCheckpointSeq,
                    (seqId, row) -> {
                        batch.add(seqId, row);
                        if (batch.isFull()) {
                            flushBatch(split, batch, transforms, sink, counters, checkpoint);
                        }
                    },
                    (seqId, error) -> {
                        flushBatch(split, batch, transforms, sink, counters, checkpoint);
                        return handleRecordError(errorPolicy, out, counters, seqId, error);
                    });
            return null;
        });
        flushBatch(split, batch, transforms, sink, counters, checkpoint);
        return new SplitResult(
                counters.rowsRead,
                counters.rowsWritten,
                counters.rowsFailed,
                counters.maxSeqId,
                counters.sinkBatches);
    }

    private boolean handleRecordError(
            PipelineConfig.ErrorPolicyConfig errorPolicy,
            PrintStream out,
            SplitCounters counters,
            long seqId,
            com.kuaia.common.pipeline.PipelineExecutionException error) {
        if (!errorPolicy.shouldSkipBadRecords()) {
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
            SplitCounters counters,
            CheckpointCallback checkpoint) throws Exception {
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
        if (checkpoint != null) {
            runStage(CHECKPOINT_STAGE, () -> {
                checkpoint.onCommitted(maxSeqId);
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

    public interface CheckpointCallback {
        void onCommitted(long maxCommittedSeqId) throws Exception;
    }

    public static final class SplitResult {
        private final long rowsRead;
        private final long rowsWritten;
        private final long rowsFailed;
        private final long maxSeqId;
        private final long sinkBatches;

        public SplitResult(long rowsRead, long rowsWritten, long rowsFailed, long maxSeqId, long sinkBatches) {
            this.rowsRead = rowsRead;
            this.rowsWritten = rowsWritten;
            this.rowsFailed = rowsFailed;
            this.maxSeqId = maxSeqId;
            this.sinkBatches = sinkBatches;
        }

        public long getRowsRead() {
            return rowsRead;
        }

        public long getRowsWritten() {
            return rowsWritten;
        }

        public long getRowsFailed() {
            return rowsFailed;
        }

        public long getMaxSeqId() {
            return maxSeqId;
        }

        public long getSinkBatches() {
            return sinkBatches;
        }
    }

    private interface StageOperation<T> {
        T run() throws Exception;
    }

    private static final class SplitCounters {
        private long rowsRead;
        private long rowsWritten;
        private long rowsFailed;
        private long maxSeqId;
        private long sinkBatches;

        private void recordSinkBatch() {
            sinkBatches++;
        }

        private void recordWrittenBatch(int sourceRows, int outputRows, long batchMaxSeqId) {
            rowsRead += sourceRows;
            rowsWritten += outputRows;
            maxSeqId = batchMaxSeqId;
        }

        private void recordFailed(long seqId) {
            rowsRead++;
            rowsFailed++;
            maxSeqId = seqId;
        }
    }

    private static final class BatchBuffer {
        private final int batchSize;
        private final List<Long> seqIds = new ArrayList<>();
        private final List<BinaryRow> rows = new ArrayList<>();

        private BatchBuffer(int batchSize) {
            this.batchSize = Math.max(1, batchSize);
        }

        private void add(long seqId, BinaryRow row) {
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

        private List<BinaryRow> rows() {
            return new ArrayList<>(rows);
        }

        private void clear() {
            seqIds.clear();
            rows.clear();
        }
    }
}
