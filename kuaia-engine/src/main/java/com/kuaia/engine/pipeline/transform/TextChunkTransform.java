package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.ArrayList;
import java.util.List;

public class TextChunkTransform implements PipelineTransform {
    private static final String CHUNK_INDEX_FIELD = "chunk_index";

    private final String inputField;
    private final String outputField;
    private final int chunkSize;
    private final int overlap;
    private KuaiaRowType inputType;
    private KuaiaRowType outputType;
    private int inputOrdinal;
    private int outputOrdinal;
    private int chunkIndexOrdinal;

    public TextChunkTransform(String inputField, String outputField, int chunkSize, int overlap) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
        if (overlap < 0) {
            throw new IllegalArgumentException("overlap must not be negative");
        }
        if (overlap >= chunkSize) {
            throw new IllegalArgumentException("overlap must be smaller than chunkSize");
        }
        this.inputField = inputField;
        this.outputField = outputField;
        this.chunkSize = chunkSize;
        this.overlap = overlap;
    }

    @Override
    public KuaiaRowType outputType(KuaiaRowType inputType) throws PipelineExecutionException {
        int candidateInputOrdinal = inputType.getIndex(inputField);
        if (candidateInputOrdinal < 0) {
            throw new PipelineExecutionException("Unknown transform field: " + inputField);
        }
        if (inputType.getFieldTypes()[candidateInputOrdinal] != DataType.STRING) {
            throw new PipelineExecutionException("Transform field must be STRING: " + inputField);
        }
        if (inputType.getIndex(outputField) >= 0) {
            throw new PipelineExecutionException("Duplicate transform field: " + outputField);
        }
        if (CHUNK_INDEX_FIELD.equals(outputField) || inputType.getIndex(CHUNK_INDEX_FIELD) >= 0) {
            throw new PipelineExecutionException("Duplicate transform field: " + CHUNK_INDEX_FIELD);
        }

        String[] outputNames = new String[inputType.getFieldNames().length + 2];
        DataType[] outputTypes = new DataType[inputType.getFieldTypes().length + 2];
        System.arraycopy(inputType.getFieldNames(), 0, outputNames, 0, inputType.getFieldNames().length);
        System.arraycopy(inputType.getFieldTypes(), 0, outputTypes, 0, inputType.getFieldTypes().length);
        outputOrdinal = inputType.getFieldNames().length;
        chunkIndexOrdinal = inputType.getFieldNames().length + 1;
        outputNames[outputOrdinal] = outputField;
        outputTypes[outputOrdinal] = DataType.STRING;
        outputNames[chunkIndexOrdinal] = CHUNK_INDEX_FIELD;
        outputTypes[chunkIndexOrdinal] = DataType.LONG;

        this.inputType = inputType;
        this.outputType = new KuaiaRowType(outputNames, outputTypes);
        this.inputOrdinal = candidateInputOrdinal;
        return outputType;
    }

    @Override
    public BinaryRow apply(BinaryRow input) throws PipelineExecutionException {
        throw new PipelineExecutionException("Chunk transform requires batch execution");
    }

    @Override
    public List<BinaryRow> applyBatch(List<BinaryRow> inputs) throws PipelineExecutionException {
        List<BinaryRow> outputs = new ArrayList<>();
        for (BinaryRow input : inputs) {
            String text = input.getString(inputOrdinal);
            if (text.isEmpty()) {
                continue;
            }
            int chunkIndex = 0;
            int step = chunkSize - overlap;
            int start = 0;
            while (start < text.length()) {
                int end = Math.min(text.length(), start + chunkSize);
                outputs.add(copyInputWithChunk(input, text.substring(start, end), chunkIndex));
                if (end == text.length()) {
                    break;
                }
                start += step;
                chunkIndex++;
            }
        }
        return outputs;
    }

    private BinaryRow copyInputWithChunk(BinaryRow input, String chunk, long chunkIndex)
            throws PipelineExecutionException {
        BinaryRow output = new BinaryRow(outputType.getFieldNames().length);
        for (int i = 0; i < inputType.getFieldNames().length; i++) {
            copyValue(input, i, output, i, inputType.getFieldTypes()[i]);
        }
        output.setString(outputOrdinal, chunk);
        output.setLong(chunkIndexOrdinal, chunkIndex);
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
