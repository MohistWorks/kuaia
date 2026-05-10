package com.kuaia.engine.worker.connector.v2;

public final class BatchCommit {
    private final String sourceSplitId;
    private final long maxSeqId;
    private final int rowCount;

    public BatchCommit(String sourceSplitId, long maxSeqId, int rowCount) {
        if (sourceSplitId == null || sourceSplitId.trim().isEmpty()) {
            throw new IllegalArgumentException("sourceSplitId must not be empty");
        }
        if (maxSeqId <= 0L) {
            throw new IllegalArgumentException("maxSeqId must be greater than zero");
        }
        if (rowCount <= 0) {
            throw new IllegalArgumentException("rowCount must be greater than zero");
        }
        this.sourceSplitId = sourceSplitId;
        this.maxSeqId = maxSeqId;
        this.rowCount = rowCount;
    }

    public String getSourceSplitId() {
        return sourceSplitId;
    }

    public long getMaxSeqId() {
        return maxSeqId;
    }

    public int getRowCount() {
        return rowCount;
    }
}
