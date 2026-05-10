package com.kuaia.engine.worker.connector.v2;

import com.kuaia.engine.pipeline.PipelineExecutionException;

public interface SourceRecordErrorConsumer {
    boolean accept(long seqId, PipelineExecutionException error) throws Exception;
}
