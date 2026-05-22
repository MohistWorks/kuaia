package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.data.BinaryRow;

import java.util.List;

public interface BatchSinkWriter extends AutoCloseable {
    void open() throws Exception;

    void writeBatch(List<BinaryRow> rows) throws Exception;

    SinkCommitter committer();

    @Override
    void close() throws Exception;
}
