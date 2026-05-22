package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.common.pipeline.PipelineExecutionException;
import com.kuaia.engine.worker.connector.FileSource;

import java.util.Collections;
import java.util.List;

public class FileSourceAdapter implements SourceEnumerator {
    private final FileSource source;
    private final String splitIdPrefix;
    private final int maxRowsPerSplit;
    private List<SourceSplit> splits;

    public FileSourceAdapter(FileSource source, String splitIdPrefix, int maxRowsPerSplit) {
        if (splitIdPrefix == null || splitIdPrefix.trim().isEmpty()) {
            throw new IllegalArgumentException("splitIdPrefix must not be empty");
        }
        if (maxRowsPerSplit <= 0) {
            throw new IllegalArgumentException("maxRowsPerSplit must be greater than zero");
        }
        this.source = source;
        this.splitIdPrefix = splitIdPrefix;
        this.maxRowsPerSplit = maxRowsPerSplit;
    }

    @Override
    public void open() throws Exception {
        source.open();
        long recordCount = source.getRecordCount();
        if (recordCount == 0L) {
            splits = Collections.singletonList(new SourceSplit(splitIdPrefix));
            return;
        }
        splits = SourceSplitPlanner.planFixedSize(splitIdPrefix, recordCount, maxRowsPerSplit);
    }

    @Override
    public List<SourceSplit> enumerateSplits() throws Exception {
        ensureOpen();
        return splits;
    }

    @Override
    public BatchSourceReader createReader(SourceSplit split) throws Exception {
        ensureOpen();
        SourceSplit knownSplit = findSplit(split);
        return new FileSplitReader(source, knownSplit);
    }

    @Override
    public KuaiaRowType getRowType() {
        return source.getRowType();
    }

    @Override
    public void close() throws Exception {
        source.close();
    }

    private void ensureOpen() throws PipelineExecutionException {
        if (splits == null) {
            throw new PipelineExecutionException("File source adapter is not open");
        }
    }

    private SourceSplit findSplit(SourceSplit split) throws PipelineExecutionException {
        for (SourceSplit knownSplit : splits) {
            if (sameSplit(knownSplit, split)) {
                return knownSplit;
            }
        }
        throw new PipelineExecutionException("Unknown source split: " + split.getSplitId());
    }

    private boolean sameSplit(SourceSplit left, SourceSplit right) {
        return left.getSplitId().equals(right.getSplitId())
                && left.getStartSeqInclusive() == right.getStartSeqInclusive()
                && left.getEndSeqInclusive() == right.getEndSeqInclusive();
    }

    private static final class FileSplitReader implements BatchSourceReader {
        private final FileSource source;
        private final SourceSplit split;

        private FileSplitReader(FileSource source, SourceSplit split) {
            this.source = source;
            this.split = split;
        }

        @Override
        public int readFrom(
                long lastCheckpointSeq,
                SourceRecordConsumer consumer,
                SourceRecordErrorConsumer errorConsumer) throws Exception {
            long effectiveCheckpoint = Math.max(lastCheckpointSeq, split.getStartSeqInclusive() - 1L);
            return source.readRange(
                    effectiveCheckpoint,
                    split.getEndSeqInclusive(),
                    consumer::accept,
                    errorConsumer::accept);
        }

        @Override
        public KuaiaRowType getRowType() {
            return source.getRowType();
        }
    }
}
