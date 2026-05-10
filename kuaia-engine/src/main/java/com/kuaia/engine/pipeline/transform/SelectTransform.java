package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SelectTransform implements PipelineTransform {
    private final List<String> fields;
    private int[] inputOrdinals;
    private KuaiaRowType outputType;

    public SelectTransform(List<String> fields) {
        this.fields = fields;
    }

    @Override
    public KuaiaRowType outputType(KuaiaRowType inputType) throws PipelineExecutionException {
        Set<String> seen = new HashSet<>();
        inputOrdinals = new int[fields.size()];
        String[] outputNames = new String[fields.size()];
        DataType[] outputTypes = new DataType[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (!seen.add(field)) {
                throw new PipelineExecutionException("Duplicate transform field: " + field);
            }
            int ordinal = inputType.getIndex(field);
            if (ordinal < 0) {
                throw new PipelineExecutionException("Unknown transform field: " + field);
            }
            inputOrdinals[i] = ordinal;
            outputNames[i] = field;
            outputTypes[i] = inputType.getFieldTypes()[ordinal];
        }
        outputType = new KuaiaRowType(outputNames, outputTypes);
        return outputType;
    }

    @Override
    public BinaryRow apply(BinaryRow input) throws PipelineExecutionException {
        BinaryRow output = new BinaryRow(inputOrdinals.length);
        for (int i = 0; i < inputOrdinals.length; i++) {
            copyValue(input, inputOrdinals[i], output, i, outputType.getFieldTypes()[i]);
        }
        return output;
    }

    private void copyValue(BinaryRow input, int inputOrdinal, BinaryRow output, int outputOrdinal, DataType type)
            throws PipelineExecutionException {
        switch (type) {
            case LONG:
                output.setLong(outputOrdinal, input.getLong(inputOrdinal));
                break;
            case STRING:
                output.setString(outputOrdinal, input.getString(inputOrdinal));
                break;
            case VECTOR:
                output.setVector(outputOrdinal, input.getVector(inputOrdinal));
                break;
            default:
                throw new PipelineExecutionException("Unsupported transform data type: " + type);
        }
    }
}
