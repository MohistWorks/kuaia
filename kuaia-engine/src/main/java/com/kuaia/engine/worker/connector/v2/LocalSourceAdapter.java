package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.worker.connector.LocalSource;

import java.util.Collections;
import java.util.List;

public class LocalSourceAdapter implements SourceEnumerator, BatchSourceReader {
    private final LocalSource source;
    private final SourceSplit split;

    public LocalSourceAdapter(LocalSource source, String splitId) {
        this.source = source;
        this.split = new SourceSplit(splitId);
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
        if (!this.split.getSplitId().equals(split.getSplitId())) {
            throw new PipelineExecutionException("Unknown source split: " + split.getSplitId());
        }
        return this;
    }

    @Override
    public int readFrom(
            long lastCheckpointSeq,
            SourceRecordConsumer consumer,
            SourceRecordErrorConsumer errorConsumer) throws Exception {
        return source.readFrom(lastCheckpointSeq, consumer::accept, errorConsumer::accept);
    }

    @Override
    public KuaiaRowType getRowType() {
        return source.getRowType();
    }

    @Override
    public void close() throws Exception {
        source.close();
    }
}
