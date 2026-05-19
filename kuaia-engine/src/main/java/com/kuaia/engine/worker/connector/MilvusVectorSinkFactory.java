package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.io.PrintStream;

public class MilvusVectorSinkFactory implements VectorSinkFactory {
    @Override
    public SinkWriter create(
            KuaiaRowType rowType,
            PrintStream out,
            PipelineConfig.SinkConfig config) throws PipelineExecutionException {
        if (config == null) {
            throw new PipelineExecutionException("Missing milvus sink config");
        }
        return new MilvusVectorSink(rowType, config);
    }
}
