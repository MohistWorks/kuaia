package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.pipeline.embedding.EmbeddingProvider;

import java.util.ArrayList;
import java.util.List;

public class EmbeddingTransform implements PipelineTransform {
    private final String inputField;
    private final String outputField;
    private final int dimensions;
    private final EmbeddingProvider provider;
    private final int batchSize;
    private KuaiaRowType inputType;
    private KuaiaRowType outputType;
    private int inputOrdinal;
    private int outputOrdinal;

    public EmbeddingTransform(String inputField, String outputField, int dimensions, EmbeddingProvider provider) {
        this(inputField, outputField, dimensions, provider, 32);
    }

    public EmbeddingTransform(
            String inputField,
            String outputField,
            int dimensions,
            EmbeddingProvider provider,
            int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.inputField = inputField;
        this.outputField = outputField;
        this.dimensions = dimensions;
        this.provider = provider;
        this.batchSize = batchSize;
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

        String[] outputNames = new String[inputType.getFieldNames().length + 1];
        DataType[] outputTypes = new DataType[inputType.getFieldTypes().length + 1];
        System.arraycopy(inputType.getFieldNames(), 0, outputNames, 0, inputType.getFieldNames().length);
        System.arraycopy(inputType.getFieldTypes(), 0, outputTypes, 0, inputType.getFieldTypes().length);
        outputOrdinal = outputNames.length - 1;
        outputNames[outputOrdinal] = outputField;
        outputTypes[outputOrdinal] = DataType.VECTOR;

        this.inputType = inputType;
        this.outputType = new KuaiaRowType(outputNames, outputTypes);
        this.inputOrdinal = candidateInputOrdinal;
        return outputType;
    }

    @Override
    public BinaryRow apply(BinaryRow input) throws PipelineExecutionException {
        BinaryRow output = copyInput(input);
        output.setVector(outputOrdinal, provider.embed(input.getString(inputOrdinal), dimensions));
        return output;
    }

    @Override
    public List<BinaryRow> applyBatch(List<BinaryRow> inputs) throws PipelineExecutionException {
        List<String> texts = new ArrayList<>();
        for (BinaryRow input : inputs) {
            texts.add(input.getString(inputOrdinal));
        }
        List<float[]> vectors = provider.embedBatch(texts, dimensions);
        if (vectors.size() != inputs.size()) {
            throw new PipelineExecutionException("Embedding provider returned " + vectors.size()
                    + " vectors for " + inputs.size() + " inputs");
        }
        List<BinaryRow> outputs = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            BinaryRow output = copyInput(inputs.get(i));
            output.setVector(outputOrdinal, vectors.get(i));
            outputs.add(output);
        }
        return outputs;
    }

    @Override
    public int preferredBatchSize() {
        return batchSize;
    }

    private BinaryRow copyInput(BinaryRow input) throws PipelineExecutionException {
        BinaryRow output = new BinaryRow(outputType.getFieldNames().length);
        for (int i = 0; i < inputType.getFieldNames().length; i++) {
            copyValue(input, i, output, i, inputType.getFieldTypes()[i]);
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
