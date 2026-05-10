package com.kuaia.engine.worker.connector.v2;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SourceSplitPlannerTest {
    @Test
    void plansFixedSizeContiguousRanges() {
        List<SourceSplit> splits = SourceSplitPlanner.planFixedSize("file-0", 5L, 2);

        assertEquals(3, splits.size());
        assertSplit(splits.get(0), "file-0-part-0", 1L, 2L);
        assertSplit(splits.get(1), "file-0-part-1", 3L, 4L);
        assertSplit(splits.get(2), "file-0-part-2", 5L, 5L);
    }

    @Test
    void plansSingleExactRangeWhenRowsFitInOneSplit() {
        List<SourceSplit> splits = SourceSplitPlanner.planFixedSize("postgres-0", 3L, 10);

        assertEquals(1, splits.size());
        assertSplit(splits.get(0), "postgres-0-part-0", 1L, 3L);
    }

    @Test
    void rejectsInvalidPlanningArguments() {
        assertThrows(IllegalArgumentException.class, () -> SourceSplitPlanner.planFixedSize("", 5L, 2));
        assertThrows(IllegalArgumentException.class, () -> SourceSplitPlanner.planFixedSize("file-0", 0L, 2));
        assertThrows(IllegalArgumentException.class, () -> SourceSplitPlanner.planFixedSize("file-0", 5L, 0));
    }

    @Test
    void plannedSplitListIsImmutable() {
        List<SourceSplit> splits = SourceSplitPlanner.planFixedSize("file-0", 2L, 1);

        assertThrows(UnsupportedOperationException.class, () -> splits.add(new SourceSplit("file-0-part-2")));
    }

    private void assertSplit(SourceSplit split, String splitId, long startSeqInclusive, long endSeqInclusive) {
        assertEquals(splitId, split.getSplitId());
        assertEquals(startSeqInclusive, split.getStartSeqInclusive());
        assertEquals(endSeqInclusive, split.getEndSeqInclusive());
    }
}
