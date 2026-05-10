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

class ConnectorV2AdapterTest {
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
