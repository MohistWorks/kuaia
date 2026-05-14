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
    private static final String OP_STARTS_WITH = "starts-with";
    private static final String OP_ENDS_WITH = "ends-with";

    private final String field;
    private final String op;
    private final int minLength;
    private final String expectedValue;
    private KuaiaRowType outputType;
    private int fieldOrdinal;

    public FilterTransform(String field, String op) {
        this(field, op, 0, null);
    }

    public FilterTransform(String field, String op, int minLength) {
        this(field, op, minLength, null);
    }

    public FilterTransform(String field, String op, int minLength, String expectedValue) {
        this.field = field;
        this.op = op;
        this.minLength = minLength;
        this.expectedValue = expectedValue;
    }

    @Override
    public KuaiaRowType outputType(KuaiaRowType inputType) throws PipelineExecutionException {
        if (!OP_NOT_EMPTY.equals(op)
                && !OP_MIN_LENGTH.equals(op)
                && !OP_CONTAINS.equals(op)
                && !OP_STARTS_WITH.equals(op)
                && !OP_ENDS_WITH.equals(op)) {
            throw new PipelineExecutionException("Unsupported filter op: " + op);
        }
        if (requiresValue() && (expectedValue == null || expectedValue.isEmpty())) {
            throw new PipelineExecutionException("Filter value is required for " + op);
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
        if (OP_CONTAINS.equals(op)) {
            return fieldValue.contains(expectedValue);
        }
        if (OP_STARTS_WITH.equals(op)) {
            return fieldValue.startsWith(expectedValue);
        }
        return fieldValue.endsWith(expectedValue);
    }

    private boolean requiresValue() {
        return OP_CONTAINS.equals(op) || OP_STARTS_WITH.equals(op) || OP_ENDS_WITH.equals(op);
    }
}
