package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;

import java.util.List;

public class SinkWriterBatchAdapter implements BatchSinkWriter {
    private final SinkWriter sink;
    private final SinkCommitter committer;

    public SinkWriterBatchAdapter(SinkWriter sink) {
        this(sink, NoopSinkCommitter.INSTANCE);
    }

    public SinkWriterBatchAdapter(SinkWriter sink, SinkCommitter committer) {
        this.sink = sink;
        this.committer = committer;
    }

    @Override
    public void open() throws Exception {
        sink.open();
    }

    @Override
    public void writeBatch(List<BinaryRow> rows) throws Exception {
        sink.writeBatch(rows);
    }

    @Override
    public SinkCommitter committer() {
        return committer;
    }

    @Override
    public void close() throws Exception {
        sink.close();
    }
}
