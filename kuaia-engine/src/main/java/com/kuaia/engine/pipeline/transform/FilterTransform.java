package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.ArrayList;
import java.util.List;

public class FilterTransform implements PipelineTransform {
    private static final String OP_NOT_EMPTY = "not-empty";
    private static final String OP_MIN_LENGTH = "min-length";
    private static final String OP_CONTAINS = "contains";

    private final String field;
    private final String op;
    private final int minLength;
    private final String expectedSubstring;
    private KuaiaRowType outputType;
    private int fieldOrdinal;

    public FilterTransform(String field, String op) {
        this(field, op, 0, null);
    }

    public FilterTransform(String field, String op, int minLength) {
        this(field, op, minLength, null);
    }

    public FilterTransform(String field, String op, int minLength, String expectedSubstring) {
        this.field = field;
        this.op = op;
        this.minLength = minLength;
        this.expectedSubstring = expectedSubstring;
    }

    @Override
    public KuaiaRowType outputType(KuaiaRowType inputType) throws PipelineExecutionException {
        if (!OP_NOT_EMPTY.equals(op) && !OP_MIN_LENGTH.equals(op) && !OP_CONTAINS.equals(op)) {
            throw new PipelineExecutionException("Unsupported filter op: " + op);
        }
        if (OP_CONTAINS.equals(op) && (expectedSubstring == null || expectedSubstring.isEmpty())) {
            throw new PipelineExecutionException("Filter value is required for contains");
        }
        int ordinal = inputType.getIndex(field);
        if (ordinal < 0) {
            throw new PipelineExecutionException("Unknown transform field: " + field);
        }
        if (inputType.getFieldTypes()[ordinal] != DataType.STRING) {
            throw new PipelineExecutionException("Transform field must be STRING: " + field);
        }
        fieldOrdinal = ordinal;
        outputType = inputType;
        return outputType;
    }

    @Override
    public BinaryRow apply(BinaryRow input) throws PipelineExecutionException {
        throw new PipelineExecutionException("Filter transform requires batch execution");
    }

    @Override
    public List<BinaryRow> applyBatch(List<BinaryRow> inputs) {
        List<BinaryRow> outputs = new ArrayList<>();
        for (BinaryRow input : inputs) {
            String fieldValue = input.getString(fieldOrdinal);
            if (passes(fieldValue)) {
                outputs.add(input);
            }
        }
        return outputs;
    }

    private boolean passes(String fieldValue) {
        if (fieldValue == null) {
            return false;
        }
        String trimmed = fieldValue.trim();
        if (OP_NOT_EMPTY.equals(op)) {
            return !trimmed.isEmpty();
        }
        if (OP_MIN_LENGTH.equals(op)) {
            return trimmed.length() >= minLength;
        }
        return fieldValue.contains(expectedSubstring);
    }
}
