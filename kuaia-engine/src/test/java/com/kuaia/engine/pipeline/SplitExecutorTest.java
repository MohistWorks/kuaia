package com.kuaia.engine.pipeline;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.pipeline.PipelineExecutionException;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import com.kuaia.engine.pipeline.transform.TransformPipeline;
import com.kuaia.engine.worker.connector.v2.BatchCommit;
import com.kuaia.engine.worker.connector.v2.BatchSinkWriter;
import com.kuaia.engine.worker.connector.v2.BatchSourceReader;
import com.kuaia.engine.worker.connector.v2.SinkCommitter;
import com.kuaia.engine.worker.connector.v2.SourceEnumerator;
import com.kuaia.engine.worker.connector.v2.SourceRecordConsumer;
import com.kuaia.engine.worker.connector.v2.SourceRecordErrorConsumer;
import com.kuaia.engine.worker.connector.v2.SourceSplit;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SplitExecutorTest {
    private static final KuaiaRowType ROW_TYPE =
            new KuaiaRowType(new String[]{"id"}, new DataType[]{DataType.LONG});

    @Test
    void batchesRowsAndCommitsMonotonicCheckpoint() throws Exception {
        SplitExecutor executor = new SplitExecutor();
        FakeEnumerator source = new FakeEnumerator(1L, 2L, 3L);
        FakeSink sink = new FakeSink();
        source.open();
        sink.open();
        TransformPipeline transforms = identityPipeline();
        List<Long> committed = new ArrayList<>();
        PrintStream out = new PrintStream(new ByteArrayOutputStream());

        SplitExecutor.SplitResult result = executor.execute(
                source,
                transforms,
                sink,
                new SourceSplit("split-0"),
                0L,
                new PipelineConfig.ErrorPolicyConfig("fail-fast"),
                out,
                committed::add);

        assertEquals(3L, result.getRowsRead());
        assertEquals(3L, result.getRowsWritten());
        assertEquals(0L, result.getRowsFailed());
        assertEquals(3L, result.getMaxSeqId());
        assertEquals(3L, committed.get(committed.size() - 1));
        assertEquals(List.of(1L, 2L, 3L), sink.writtenSeqIds);
    }

    @Test
    void resumesFromLastCheckpointSeq() throws Exception {
        SplitExecutor executor = new SplitExecutor();
        FakeEnumerator source = new FakeEnumerator(1L, 2L, 3L);
        FakeSink sink = new FakeSink();
        source.open();
        sink.open();
        TransformPipeline transforms = identityPipeline();
        List<Long> committed = new ArrayList<>();
        PrintStream out = new PrintStream(new ByteArrayOutputStream());

        SplitExecutor.SplitResult result = executor.execute(
                source,
                transforms,
                sink,
                new SourceSplit("split-0"),
                2L,
                new PipelineConfig.ErrorPolicyConfig("fail-fast"),
                out,
                committed::add);

        assertEquals(1L, result.getRowsRead());
        assertEquals(1L, result.getRowsWritten());
        assertEquals(3L, result.getMaxSeqId());
        assertEquals(List.of(3L), sink.writtenSeqIds);
        assertEquals(3L, committed.get(committed.size() - 1));
    }

    @Test
    void skipsBadRecordsUnderSkipPolicy() throws Exception {
        SplitExecutor executor = new SplitExecutor();
        FakeEnumerator source = new FakeEnumerator(1L, 2L, 3L);
        source.failSeqId = 2L;
        FakeSink sink = new FakeSink();
        source.open();
        sink.open();
        TransformPipeline transforms = identityPipeline();
        List<Long> committed = new ArrayList<>();
        PrintStream out = new PrintStream(new ByteArrayOutputStream());

        SplitExecutor.SplitResult result = executor.execute(
                source,
                transforms,
                sink,
                new SourceSplit("split-0"),
                0L,
                new PipelineConfig.ErrorPolicyConfig("skip-bad-records"),
                out,
                committed::add);

        assertEquals(1L, result.getRowsFailed());
        assertEquals(2L, result.getRowsWritten());
        assertTrue(sink.writtenSeqIds.contains(1L));
        assertTrue(sink.writtenSeqIds.contains(3L));
    }

    @Test
    void tagsSinkStageFailures() throws Exception {
        SplitExecutor executor = new SplitExecutor();
        FakeEnumerator source = new FakeEnumerator(1L);
        FakeSink sink = new FakeSink();
        sink.failOnWrite = true;
        source.open();
        sink.open();
        TransformPipeline transforms = identityPipeline();
        PrintStream out = new PrintStream(new ByteArrayOutputStream());

        Exception error = assertThrows(
                Exception.class,
                () -> executor.execute(
                        source,
                        transforms,
                        sink,
                        new SourceSplit("split-0"),
                        0L,
                        new PipelineConfig.ErrorPolicyConfig("fail-fast"),
                        out,
                        seq -> {}));

        assertTrue(
                error.getMessage().startsWith("Sink stage failed:"),
                "expected sink stage tag, was: " + error.getMessage());
    }

    private static TransformPipeline identityPipeline() throws PipelineExecutionException {
        return TransformPipeline.from(ROW_TYPE, List.of(), EmbeddingProviderRegistry.defaultRegistry());
    }

    private static BinaryRow longRow(long value) {
        BinaryRow row = new BinaryRow(1);
        row.setLong(0, value);
        return row;
    }

    private static final class FakeEnumerator implements SourceEnumerator {
        private final long[] seqIds;
        private long failSeqId = -1L;

        private FakeEnumerator(long... seqIds) {
            this.seqIds = seqIds;
        }

        @Override
        public void open() {
        }

        @Override
        public List<SourceSplit> enumerateSplits() {
            return List.of(new SourceSplit("split-0"));
        }

        @Override
        public BatchSourceReader createReader(SourceSplit split) {
            return new FakeReader();
        }

        @Override
        public KuaiaRowType getRowType() {
            return ROW_TYPE;
        }

        @Override
        public void close() {
        }

        private final class FakeReader implements BatchSourceReader {
            @Override
            public int readFrom(
                    long lastCheckpointSeq,
                    SourceRecordConsumer consumer,
                    SourceRecordErrorConsumer errorConsumer) throws Exception {
                int read = 0;
                for (long seqId : seqIds) {
                    if (seqId <= lastCheckpointSeq) {
                        continue;
                    }
                    if (seqId == failSeqId) {
                        errorConsumer.accept(
                                seqId,
                                new PipelineExecutionException("bad record seq=" + seqId));
                    } else {
                        consumer.accept(seqId, longRow(seqId));
                    }
                    read++;
                }
                return read;
            }

            @Override
            public KuaiaRowType getRowType() {
                return ROW_TYPE;
            }
        }
    }

    private static final class FakeSink implements BatchSinkWriter, SinkCommitter {
        private final List<Long> writtenSeqIds = new ArrayList<>();
        private boolean failOnWrite = false;

        @Override
        public void open() {
        }

        @Override
        public void writeBatch(List<BinaryRow> rows) {
            if (failOnWrite) {
                throw new RuntimeException("sink batch failed");
            }
            for (BinaryRow row : rows) {
                writtenSeqIds.add(row.getLong(0));
            }
        }

        @Override
        public SinkCommitter committer() {
            return this;
        }

        @Override
        public void commit(BatchCommit commit) {
        }

        @Override
        public void close() {
        }
    }
}
