package com.kuaia.engine.worker.connector.v2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class SourceSplitPlanner {
    private SourceSplitPlanner() {}

    public static List<SourceSplit> planFixedSize(
            String splitIdPrefix,
            long totalRows,
            int maxRowsPerSplit) {
        if (splitIdPrefix == null || splitIdPrefix.trim().isEmpty()) {
            throw new IllegalArgumentException("splitIdPrefix must not be empty");
        }
        if (totalRows <= 0L) {
            throw new IllegalArgumentException("totalRows must be greater than zero");
        }
        if (maxRowsPerSplit <= 0) {
            throw new IllegalArgumentException("maxRowsPerSplit must be greater than zero");
        }

        List<SourceSplit> splits = new ArrayList<>();
        long startSeqInclusive = 1L;
        int part = 0;
        while (startSeqInclusive <= totalRows) {
            long rowsRemaining = totalRows - startSeqInclusive + 1L;
            long endSeqInclusive = rowsRemaining <= maxRowsPerSplit
                    ? totalRows
                    : startSeqInclusive + maxRowsPerSplit - 1L;
            splits.add(new SourceSplit(
                    splitIdPrefix + "-part-" + part,
                    startSeqInclusive,
                    endSeqInclusive));
            startSeqInclusive = endSeqInclusive + 1L;
            part++;
        }
        return Collections.unmodifiableList(splits);
    }
}
