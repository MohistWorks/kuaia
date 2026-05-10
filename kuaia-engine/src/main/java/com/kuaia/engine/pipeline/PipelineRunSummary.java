package com.kuaia.engine.pipeline;

import com.kuaia.common.model.TaskState;

public class PipelineRunSummary {
    private final long rowsRead;
    private final long rowsWritten;
    private final long rowsFailed;
    private final long rowsSkipped;
    private final long checkpointSeq;
    private final TaskState taskState;
    private final long sourceSplits;
    private final long sinkBatches;
    private final long durationMillis;

    public PipelineRunSummary(
            long rowsRead,
            long rowsWritten,
            long rowsFailed,
            long rowsSkipped,
            long checkpointSeq,
            TaskState taskState,
            long durationMillis) {
        this(
                rowsRead,
                rowsWritten,
                rowsFailed,
                rowsSkipped,
                checkpointSeq,
                taskState,
                0L,
                0L,
                durationMillis);
    }

    public PipelineRunSummary(
            long rowsRead,
            long rowsWritten,
            long rowsFailed,
            long rowsSkipped,
            long checkpointSeq,
            TaskState taskState,
            long sourceSplits,
            long sinkBatches,
            long durationMillis) {
        this.rowsRead = rowsRead;
        this.rowsWritten = rowsWritten;
        this.rowsFailed = rowsFailed;
        this.rowsSkipped = rowsSkipped;
        this.checkpointSeq = checkpointSeq;
        this.taskState = taskState;
        this.sourceSplits = sourceSplits;
        this.sinkBatches = sinkBatches;
        this.durationMillis = durationMillis;
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

    public long getRowsSkipped() {
        return rowsSkipped;
    }

    public long getCheckpointSeq() {
        return checkpointSeq;
    }

    public TaskState getTaskState() {
        return taskState;
    }

    public long getSourceSplits() {
        return sourceSplits;
    }

    public long getSinkBatches() {
        return sinkBatches;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public PipelineRunSummary withDurationMillis(long durationMillis) {
        return new PipelineRunSummary(
                rowsRead,
                rowsWritten,
                rowsFailed,
                rowsSkipped,
                checkpointSeq,
                taskState,
                sourceSplits,
                sinkBatches,
                durationMillis);
    }

    public String toCliLine() {
        return "Run Summary: rowsRead="
                + rowsRead
                + " rowsWritten="
                + rowsWritten
                + " rowsFailed="
                + rowsFailed
                + " rowsSkipped="
                + rowsSkipped
                + " checkpointSeq="
                + checkpointSeq
                + " taskState="
                + taskState
                + " sourceSplits="
                + sourceSplits
                + " sinkBatches="
                + sinkBatches
                + " durationMs="
                + durationMillis;
    }
}
