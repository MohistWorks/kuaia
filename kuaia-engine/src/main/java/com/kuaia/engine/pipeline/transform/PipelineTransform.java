package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

public interface PipelineTransform {
    KuaiaRowType outputType(KuaiaRowType inputType) throws PipelineExecutionException;

    BinaryRow apply(BinaryRow input) throws PipelineExecutionException;
}
