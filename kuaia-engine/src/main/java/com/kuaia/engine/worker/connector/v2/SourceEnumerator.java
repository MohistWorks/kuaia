package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.type.KuaiaRowType;

import java.util.List;

public interface SourceEnumerator extends AutoCloseable {
    void open() throws Exception;

    List<SourceSplit> enumerateSplits() throws Exception;

    BatchSourceReader createReader(SourceSplit split) throws Exception;

    KuaiaRowType getRowType();

    @Override
    void close() throws Exception;
}
