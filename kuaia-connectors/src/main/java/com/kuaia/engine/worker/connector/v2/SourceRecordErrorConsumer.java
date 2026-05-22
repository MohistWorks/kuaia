package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.pipeline.PipelineExecutionException;

public interface SourceRecordErrorConsumer {
    boolean accept(long seqId, PipelineExecutionException error) throws Exception;
}
