package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.Locale;

public class LowercaseTransform implements PipelineTransform {
    private final String field;
    private int fieldOrdinal;

    public LowercaseTransform(String field) {
        this.field = field;
    }

    @Override
    public KuaiaRowType outputType(KuaiaRowType inputType) throws PipelineExecutionException {
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
        input.setString(fieldOrdinal, input.getString(fieldOrdinal).toLowerCase(Locale.ROOT));
        return input;
    }
}
