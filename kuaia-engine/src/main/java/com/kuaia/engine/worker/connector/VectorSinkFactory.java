package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.io.PrintStream;

public interface VectorSinkFactory {
    SinkWriter create(KuaiaRowType rowType, PrintStream out) throws PipelineExecutionException;
}
