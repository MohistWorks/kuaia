package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.ArrayList;
import java.util.List;

public class TextChunkTransform implements PipelineTransform {
    private static final String CHUNK_INDEX_FIELD = "chunk_index";
    private static final String CHUNK_START_FIELD = "chunk_start";
    private static final String CHUNK_END_FIELD = "chunk_end";

    private final String inputField;
    private final String outputField;
    private final int chunkSize;
    private final int overlap;
    private final boolean dropInput;
    private final boolean includeOffsets;
    private KuaiaRowType inputType;
    private KuaiaRowType outputType;
    private int inputOrdinal;
    private int outputOrdinal;
    private int chunkIndexOrdinal;
    private int chunkStartOrdinal;
    private int chunkEndOrdinal;

    public TextChunkTransform(
            String inputField,
            String outputField,
            int chunkSize,
            int overlap,
            boolean dropInput,
            boolean includeOffsets) {
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
        this.dropInput = dropInput;
        this.includeOffsets = includeOffsets;
        this.chunkStartOrdinal = -1;
        this.chunkEndOrdinal = -1;
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
        if (includeOffsets) {
            rejectDuplicateOffsetField(inputType, outputField, CHUNK_START_FIELD);
            rejectDuplicateOffsetField(inputType, outputField, CHUNK_END_FIELD);
        }

        int inputFieldCount = inputType.getFieldNames().length;
        int retainedInputFields = dropInput ? inputFieldCount - 1 : inputFieldCount;
        int extraFields = includeOffsets ? 4 : 2;
        String[] outputNames = new String[retainedInputFields + extraFields];
        DataType[] outputTypes = new DataType[retainedInputFields + extraFields];

        int cursor = 0;
        for (int i = 0; i < inputFieldCount; i++) {
            if (dropInput && i == candidateInputOrdinal) {
                continue;
            }
            outputNames[cursor] = inputType.getFieldNames()[i];
            outputTypes[cursor] = inputType.getFieldTypes()[i];
            cursor++;
        }
        outputOrdinal = cursor++;
        chunkIndexOrdinal = cursor++;
        outputNames[outputOrdinal] = outputField;
        outputTypes[outputOrdinal] = DataType.STRING;
        outputNames[chunkIndexOrdinal] = CHUNK_INDEX_FIELD;
        outputTypes[chunkIndexOrdinal] = DataType.LONG;
        if (includeOffsets) {
            chunkStartOrdinal = cursor++;
            chunkEndOrdinal = cursor;
            outputNames[chunkStartOrdinal] = CHUNK_START_FIELD;
            outputTypes[chunkStartOrdinal] = DataType.LONG;
            outputNames[chunkEndOrdinal] = CHUNK_END_FIELD;
            outputTypes[chunkEndOrdinal] = DataType.LONG;
        }

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
                outputs.add(copyInputWithChunk(input, text.substring(start, end), chunkIndex, start, end));
                if (end == text.length()) {
                    break;
                }
                start += step;
                chunkIndex++;
            }
        }
        return outputs;
    }

    private void rejectDuplicateOffsetField(KuaiaRowType inputType, String outputField, String offsetField)
            throws PipelineExecutionException {
        if (offsetField.equals(outputField) || inputType.getIndex(offsetField) >= 0) {
            throw new PipelineExecutionException("Duplicate transform field: " + offsetField);
        }
    }

    private BinaryRow copyInputWithChunk(BinaryRow input, String chunk, long chunkIndex, long chunkStart, long chunkEnd)
            throws PipelineExecutionException {
        BinaryRow output = new BinaryRow(outputType.getFieldNames().length);
        int cursor = 0;
        for (int i = 0; i < inputType.getFieldNames().length; i++) {
            if (dropInput && i == inputOrdinal) {
                continue;
            }
            copyValue(input, i, output, cursor, inputType.getFieldTypes()[i]);
            cursor++;
        }
        output.setString(outputOrdinal, chunk);
        output.setLong(chunkIndexOrdinal, chunkIndex);
        if (includeOffsets) {
            output.setLong(chunkStartOrdinal, chunkStart);
            output.setLong(chunkEndOrdinal, chunkEnd);
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
