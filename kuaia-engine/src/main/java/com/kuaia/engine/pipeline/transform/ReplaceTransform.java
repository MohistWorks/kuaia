package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

public class ReplaceTransform implements PipelineTransform {
    private final String field;
    private final String target;
    private final String replacement;
    private int fieldOrdinal;

    public ReplaceTransform(String field, String target, String replacement) {
        this.field = field;
        this.target = target;
        this.replacement = replacement == null ? "" : replacement;
    }

    @Override
    public KuaiaRowType outputType(KuaiaRowType inputType) throws PipelineExecutionException {
        if (target == null || target.isEmpty()) {
            throw new PipelineExecutionException("Replace target must not be empty");
        }
        int ordinal = inputType.getIndex(field);
        if (ordinal < 0) {
            throw new PipelineExecutionException("Unknown transform field: " + field);
        }
        if (inputType.getFieldTypes()[ordinal] != DataType.STRING) {
            throw new PipelineExecutionException("Transform field must be STRING: " + field);
        }
        fieldOrdinal = ordinal;
        return inputType;
    }

    @Override
    public BinaryRow apply(BinaryRow input) {
        input.setString(fieldOrdinal, input.getString(fieldOrdinal).replace(target, replacement));
        return input;
    }
}
