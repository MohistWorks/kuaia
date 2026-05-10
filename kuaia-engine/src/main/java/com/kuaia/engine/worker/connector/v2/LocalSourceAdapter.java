package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.worker.connector.LocalSource;

import java.util.Collections;
import java.util.List;

public class LocalSourceAdapter implements SourceEnumerator {
    private final LocalSource source;
    private final SourceSplit split;

    public LocalSourceAdapter(LocalSource source, String splitId) {
        this(source, new SourceSplit(splitId));
    }

    public LocalSourceAdapter(LocalSource source, SourceSplit split) {
        this.source = source;
        this.split = split;
    }

    @Override
    public void open() throws Exception {
        source.open();
    }

    @Override
    public List<SourceSplit> enumerateSplits() {
        return Collections.singletonList(split);
    }

    @Override
    public BatchSourceReader createReader(SourceSplit split) throws PipelineExecutionException {
        if (!sameSplit(split)) {
            throw new PipelineExecutionException("Unknown source split: " + split.getSplitId());
        }
        return new SplitReader(source, split);
    }

    private boolean sameSplit(SourceSplit other) {
        return this.split.getSplitId().equals(other.getSplitId())
                && this.split.getStartSeqInclusive() == other.getStartSeqInclusive()
                && this.split.getEndSeqInclusive() == other.getEndSeqInclusive();
    }

    @Override
    public KuaiaRowType getRowType() {
        return source.getRowType();
    }

    @Override
    public void close() throws Exception {
        source.close();
    }

    private static final class SplitReader implements BatchSourceReader {
        private final LocalSource source;
        private final SourceSplit split;

        private SplitReader(LocalSource source, SourceSplit split) {
            this.source = source;
            this.split = split;
        }

        @Override
        public int readFrom(
                long lastCheckpointSeq,
                SourceRecordConsumer consumer,
                SourceRecordErrorConsumer errorConsumer) throws Exception {
            final int[] accepted = new int[]{0};
            long effectiveCheckpoint = Math.max(lastCheckpointSeq, split.getStartSeqInclusive() - 1L);
            source.readFrom(
                    effectiveCheckpoint,
                    (seqId, row) -> {
                        if (seqId > split.getEndSeqInclusive()) {
                            return;
                        }
                        consumer.accept(seqId, row);
                        accepted[0]++;
                    },
                    (seqId, error) -> {
                        if (seqId > split.getEndSeqInclusive()) {
                            return true;
                        }
                        return errorConsumer.accept(seqId, error);
                    });
            return accepted[0];
        }

        @Override
        public KuaiaRowType getRowType() {
            return source.getRowType();
        }
    }
}
