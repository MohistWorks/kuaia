package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

public class RenameTransform implements PipelineTransform {
    private final String from;
    private final String to;

    public RenameTransform(String from, String to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public KuaiaRowType outputType(KuaiaRowType inputType) throws PipelineExecutionException {
        int renameOrdinal = inputType.getIndex(from);
        if (renameOrdinal < 0) {
            throw new PipelineExecutionException("Unknown transform field: " + from);
        }
        int existingTargetOrdinal = inputType.getIndex(to);
        if (existingTargetOrdinal >= 0 && existingTargetOrdinal != renameOrdinal) {
            throw new PipelineExecutionException("Duplicate transform field: " + to);
        }

        String[] fieldNames = inputType.getFieldNames().clone();
        fieldNames[renameOrdinal] = to;
        return new KuaiaRowType(fieldNames, inputType.getFieldTypes().clone());
    }

    @Override
    public BinaryRow apply(BinaryRow input) {
        return input;
    }
}
