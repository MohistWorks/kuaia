package com.kuaia.engine.worker.connector.v2;

public final class SourceSplit {
    private final String splitId;
    private final long startSeqInclusive;
    private final long endSeqInclusive;

    public SourceSplit(String splitId) {
        this(splitId, 1L, Long.MAX_VALUE);
    }

    public SourceSplit(String splitId, long startSeqInclusive, long endSeqInclusive) {
        if (splitId == null || splitId.trim().isEmpty()) {
            throw new IllegalArgumentException("splitId must not be empty");
        }
        if (startSeqInclusive < 1L) {
            throw new IllegalArgumentException("startSeqInclusive must be greater than zero");
        }
        if (endSeqInclusive < startSeqInclusive) {
            throw new IllegalArgumentException("endSeqInclusive must be greater than or equal to startSeqInclusive");
        }
        this.splitId = splitId;
        this.startSeqInclusive = startSeqInclusive;
        this.endSeqInclusive = endSeqInclusive;
    }

    public String getSplitId() {
        return splitId;
    }

    public long getStartSeqInclusive() {
        return startSeqInclusive;
    }

    public long getEndSeqInclusive() {
        return endSeqInclusive;
    }
}
