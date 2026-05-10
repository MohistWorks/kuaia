package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.worker.connector.LocalSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConnectorV2AdapterTest {
    @Test
    void sourceSplitDefaultsToFullSeqRange() {
        SourceSplit split = new SourceSplit("file-0");

        assertEquals("file-0", split.getSplitId());
        assertEquals(1L, split.getStartSeqInclusive());
        assertEquals(Long.MAX_VALUE, split.getEndSeqInclusive());
    }

    @Test
    void sourceSplitRejectsInvalidRanges() {
        assertThrows(IllegalArgumentException.class, () -> new SourceSplit("", 1L, 2L));
        assertThrows(IllegalArgumentException.class, () -> new SourceSplit("file-0", 0L, 2L));
        assertThrows(IllegalArgumentException.class, () -> new SourceSplit("file-0", 3L, 2L));
    }

    @Test
    void localSourceAdapterEnumeratesSingleSplitAndResumesFromCheckpoint() throws Exception {
        RecordingSource source = new RecordingSource(rows(
                row(1L, "Alice"),
                row(2L, "Bob"),
                row(3L, "Carol")));
        LocalSourceAdapter adapter = new LocalSourceAdapter(source, "file-0");

        adapter.open();
        List<SourceSplit> splits = adapter.enumerateSplits();
        BatchSourceReader reader = adapter.createReader(splits.get(0));
        List<Long> seqIds = new ArrayList<>();
        List<String> names = new ArrayList<>();

        int read = reader.readFrom(
                1L,
                (seqId, row) -> {
                    seqIds.add(seqId);
                    names.add(row.getString(1));
                },
                (seqId, error) -> false);

        assertEquals(2, read);
        assertEquals(1, splits.size());
        assertEquals("file-0", splits.get(0).getSplitId());
        assertEquals(Arrays.asList(2L, 3L), seqIds);
        assertEquals(Arrays.asList("Bob", "Carol"), names);
        assertSame(source.getRowType(), adapter.getRowType());
        adapter.close();
        assertEquals(1, source.openCalls);
        assertEquals(1, source.closeCalls);
    }

    @Test
    void localSourceAdapterReaderHonorsSplitRangeAndCheckpoint() throws Exception {
        RecordingSource source = new RecordingSource(rows(
                row(1L, "Alice"),
                row(2L, "Bob"),
                row(3L, "Carol"),
                row(4L, "Dave"),
                row(5L, "Eve")));
        SourceSplit split = new SourceSplit("file-0-part-0", 2L, 4L);
        LocalSourceAdapter adapter = new LocalSourceAdapter(source, split);

        adapter.open();
        BatchSourceReader reader = adapter.createReader(split);
        List<Long> seqIds = new ArrayList<>();

        int read = reader.readFrom(
                0L,
                (seqId, row) -> seqIds.add(seqId),
                (seqId, error) -> false);

        assertEquals(3, read);
        assertEquals(Arrays.asList(2L, 3L, 4L), seqIds);

        seqIds.clear();
        int resumed = reader.readFrom(
                3L,
                (seqId, row) -> seqIds.add(seqId),
                (seqId, error) -> false);

        assertEquals(1, resumed);
        assertEquals(Arrays.asList(4L), seqIds);
        adapter.close();
    }

    @Test
    void localSourceAdapterRejectsUnexpectedSplitRange() {
        RecordingSource source = new RecordingSource(rows(row(1L, "Alice")));
        LocalSourceAdapter adapter = new LocalSourceAdapter(source, new SourceSplit("file-0", 2L, 4L));

        assertThrows(
                PipelineExecutionException.class,
                () -> adapter.createReader(new SourceSplit("file-0", 1L, 4L)));
    }

    @Test
    void localSourceAdapterEnumeratesMultipleSplitsInOrderAndListIsImmutable() throws Exception {
        RecordingSource source = new RecordingSource(rows(row(1L, "Alice")));
        List<SourceSplit> splits = Arrays.asList(
                new SourceSplit("file-0-part-0", 1L, 2L),
                new SourceSplit("file-0-part-1", 3L, 4L));
        LocalSourceAdapter adapter = new LocalSourceAdapter(source, splits);

        List<SourceSplit> enumerated = adapter.enumerateSplits();

        assertEquals(2, enumerated.size());
        assertEquals("file-0-part-0", enumerated.get(0).getSplitId());
        assertEquals("file-0-part-1", enumerated.get(1).getSplitId());
        assertThrows(
                UnsupportedOperationException.class,
                () -> enumerated.add(new SourceSplit("file-0-part-2", 5L, 6L)));
    }

    @Test
    void localSourceAdapterReadersIsolateMultipleSplitRanges() throws Exception {
        RecordingSource source = new RecordingSource(rows(
                row(1L, "Alice"),
                row(2L, "Bob"),
                row(3L, "Carol"),
                row(4L, "Dave")));
        List<SourceSplit> splits = Arrays.asList(
                new SourceSplit("file-0-part-0", 1L, 2L),
                new SourceSplit("file-0-part-1", 3L, 4L));
        LocalSourceAdapter adapter = new LocalSourceAdapter(source, splits);

        adapter.open();
        List<Long> firstSeqIds = new ArrayList<>();
        int firstRead = adapter.createReader(splits.get(0)).readFrom(
                0L,
                (seqId, row) -> firstSeqIds.add(seqId),
                (seqId, error) -> false);
        List<Long> secondSeqIds = new ArrayList<>();
        int secondRead = adapter.createReader(splits.get(1)).readFrom(
                0L,
                (seqId, row) -> secondSeqIds.add(seqId),
                (seqId, error) -> false);

        assertEquals(2, firstRead);
        assertEquals(Arrays.asList(1L, 2L), firstSeqIds);
        assertEquals(2, secondRead);
        assertEquals(Arrays.asList(3L, 4L), secondSeqIds);
        adapter.close();
    }

    @Test
    void localSourceAdapterRejectsEmptySplitList() {
        RecordingSource source = new RecordingSource(rows(row(1L, "Alice")));

        assertThrows(IllegalArgumentException.class, () -> new LocalSourceAdapter(source, new ArrayList<>()));
    }

    @Test
    void sinkWriterBatchAdapterWritesBatchThenCommits() throws Exception {
        RecordingSink sink = new RecordingSink();
        RecordingCommitter committer = new RecordingCommitter();
        SinkWriterBatchAdapter adapter = new SinkWriterBatchAdapter(sink, committer);
        List<BinaryRow> rows = rows(row(1L, "Alice"), row(2L, "Bob"));

        adapter.open();
        adapter.writeBatch(rows);
        adapter.committer().commit(new BatchCommit("file-0", 2L, rows.size()));
        adapter.close();

        assertEquals(1, sink.openCalls);
        assertEquals(1, sink.batchWriteCalls);
        assertSame(rows, sink.lastBatch);
        assertEquals(1, committer.commits.size());
        assertEquals("file-0", committer.commits.get(0).getSourceSplitId());
        assertEquals(2L, committer.commits.get(0).getMaxSeqId());
        assertEquals(2, committer.commits.get(0).getRowCount());
        assertEquals(1, sink.closeCalls);
    }

    private static BinaryRow row(long id, String name) {
        BinaryRow row = new BinaryRow(2);
        row.setLong(0, id);
        row.setString(1, name);
        return row;
    }

    private static List<BinaryRow> rows(BinaryRow... rows) {
        return Arrays.asList(rows);
    }

    private static class RecordingSource implements LocalSource {
        private final List<BinaryRow> rows;
        private final KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "name"},
                new DataType[]{DataType.LONG, DataType.STRING});
        private int openCalls;
        private int closeCalls;

        private RecordingSource(List<BinaryRow> rows) {
            this.rows = rows;
        }

        @Override
        public void open() {
            openCalls++;
        }

        @Override
        public int readFrom(long lastCheckpointSeq, RecordConsumer consumer, RecordErrorConsumer errorConsumer)
                throws Exception {
            int read = 0;
            for (int i = 0; i < rows.size(); i++) {
                long seqId = i + 1L;
                if (seqId <= lastCheckpointSeq) {
                    continue;
                }
                consumer.accept(seqId, rows.get(i));
                read++;
            }
            return read;
        }

        @Override
        public KuaiaRowType getRowType() {
            return rowType;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }

    private static class RecordingSink implements SinkWriter {
        private int openCalls;
        private int batchWriteCalls;
        private int closeCalls;
        private List<BinaryRow> lastBatch;

        @Override
        public void open() {
            openCalls++;
        }

        @Override
        public void write(BinaryRow row) {
            throw new AssertionError("batch adapter should not call single-row write");
        }

        @Override
        public void writeBatch(List<BinaryRow> rows) {
            batchWriteCalls++;
            lastBatch = rows;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }

    private static class RecordingCommitter implements SinkCommitter {
        private final List<BatchCommit> commits = new ArrayList<>();

        @Override
        public void commit(BatchCommit commit) throws PipelineExecutionException {
            commits.add(commit);
        }
    }
}
