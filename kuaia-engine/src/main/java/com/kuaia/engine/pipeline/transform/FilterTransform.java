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
    private static final String OP_EQUALS = "equals";
    private static final String OP_NOT_EQUALS = "not-equals";
    private static final String OP_GREATER_THAN = "greater-than";
    private static final String OP_GREATER_THAN_OR_EQUAL = "greater-than-or-equal";
    private static final String OP_LESS_THAN = "less-than";
    private static final String OP_LESS_THAN_OR_EQUAL = "less-than-or-equal";

    private final String field;
    private final String op;
    private final int minLength;
    private final String expectedValue;
    private KuaiaRowType outputType;
    private int fieldOrdinal;
    private long expectedLongValue;

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
                && !OP_ENDS_WITH.equals(op)
                && !OP_EQUALS.equals(op)
                && !OP_NOT_EQUALS.equals(op)
                && !isLongComparison()) {
            throw new PipelineExecutionException("Unsupported filter op: " + op);
        }
        if (requiresValue() && (expectedValue == null || expectedValue.isEmpty())) {
            throw new PipelineExecutionException("Filter value is required for " + op);
        }
        int ordinal = inputType.getIndex(field);
        if (ordinal < 0) {
            throw new PipelineExecutionException("Unknown transform field: " + field);
        }
        if (isLongComparison()) {
            if (inputType.getFieldTypes()[ordinal] != DataType.LONG) {
                throw new PipelineExecutionException("Transform field must be LONG: " + field);
            }
            expectedLongValue = parseExpectedLongValue();
        } else if (inputType.getFieldTypes()[ordinal] != DataType.STRING) {
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
            if (isLongComparison() && passesLong(input.getLong(fieldOrdinal))) {
                outputs.add(input);
            } else if (!isLongComparison() && passesString(input.getString(fieldOrdinal))) {
                outputs.add(input);
            }
        }
        return outputs;
    }

    private boolean passesString(String fieldValue) {
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
        if (OP_ENDS_WITH.equals(op)) {
            return fieldValue.endsWith(expectedValue);
        }
        if (OP_EQUALS.equals(op)) {
            return fieldValue.equals(expectedValue);
        }
        return !fieldValue.equals(expectedValue);
    }

    private boolean passesLong(long fieldValue) {
        if (OP_GREATER_THAN.equals(op)) {
            return fieldValue > expectedLongValue;
        }
        if (OP_GREATER_THAN_OR_EQUAL.equals(op)) {
            return fieldValue >= expectedLongValue;
        }
        if (OP_LESS_THAN.equals(op)) {
            return fieldValue < expectedLongValue;
        }
        return fieldValue <= expectedLongValue;
    }

    private boolean requiresValue() {
        return OP_CONTAINS.equals(op)
                || OP_STARTS_WITH.equals(op)
                || OP_ENDS_WITH.equals(op)
                || OP_EQUALS.equals(op)
                || OP_NOT_EQUALS.equals(op)
                || isLongComparison();
    }

    private boolean isLongComparison() {
        return OP_GREATER_THAN.equals(op)
                || OP_GREATER_THAN_OR_EQUAL.equals(op)
                || OP_LESS_THAN.equals(op)
                || OP_LESS_THAN_OR_EQUAL.equals(op);
    }

    private long parseExpectedLongValue() throws PipelineExecutionException {
        try {
            return Long.parseLong(expectedValue.trim());
        } catch (NumberFormatException e) {
            throw new PipelineExecutionException("Filter value must be LONG: " + expectedValue, e);
        }
    }
}
