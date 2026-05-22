package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.common.pipeline.PipelineExecutionException;

public interface LocalSource extends AutoCloseable {
    interface RecordConsumer {
        void accept(long seqId, BinaryRow row) throws Exception;
    }

    interface RecordErrorConsumer {
        boolean accept(long seqId, PipelineExecutionException error) throws Exception;
    }

    void open() throws Exception;

    int readFrom(long lastCheckpointSeq, RecordConsumer consumer, RecordErrorConsumer errorConsumer) throws Exception;

    KuaiaRowType getRowType();

    @Override
    void close() throws Exception;
}
