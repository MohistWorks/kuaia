package com.kuaia.engine.worker.connector.v2;

public final class SourceSplit {
    private final String splitId;

    public SourceSplit(String splitId) {
        if (splitId == null || splitId.trim().isEmpty()) {
            throw new IllegalArgumentException("splitId must not be empty");
        }
        this.splitId = splitId;
    }

    public String getSplitId() {
        return splitId;
    }
}
