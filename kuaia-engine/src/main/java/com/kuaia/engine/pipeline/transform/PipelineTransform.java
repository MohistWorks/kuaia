package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.ArrayList;
import java.util.List;

public interface PipelineTransform {
    KuaiaRowType outputType(KuaiaRowType inputType) throws PipelineExecutionException;

    BinaryRow apply(BinaryRow input) throws PipelineExecutionException;

    default List<BinaryRow> applyBatch(List<BinaryRow> inputs) throws PipelineExecutionException {
        List<BinaryRow> outputs = new ArrayList<>();
        for (BinaryRow input : inputs) {
            outputs.add(apply(input));
        }
        return outputs;
    }

    default int preferredBatchSize() {
        return Integer.MAX_VALUE;
    }
}
