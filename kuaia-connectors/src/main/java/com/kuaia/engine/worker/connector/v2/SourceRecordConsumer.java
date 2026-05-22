package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.data.BinaryRow;

public interface SourceRecordConsumer {
    void accept(long seqId, BinaryRow row) throws Exception;
}
