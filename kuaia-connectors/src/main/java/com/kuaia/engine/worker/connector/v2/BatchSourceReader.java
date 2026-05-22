package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.type.KuaiaRowType;

public interface BatchSourceReader {
    int readFrom(
            long lastCheckpointSeq,
            SourceRecordConsumer consumer,
            SourceRecordErrorConsumer errorConsumer) throws Exception;

    KuaiaRowType getRowType();
}
