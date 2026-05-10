package com.kuaia.engine.worker.connector.v2;

public final class BatchCommit {
    private final String sourceSplitId;
    private final long maxSeqId;
    private final int rowCount;

    public BatchCommit(String sourceSplitId, long maxSeqId, int rowCount) {
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
