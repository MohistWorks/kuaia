package com.kuaia.engine.pipeline.embedding;

import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.ArrayList;
import java.util.List;

public interface EmbeddingProvider {
    float[] embed(String input, int dimensions) throws PipelineExecutionException;

    default List<float[]> embedBatch(List<String> inputs, int dimensions) throws PipelineExecutionException {
        List<float[]> vectors = new ArrayList<>();
        for (String input : inputs) {
            vectors.add(embed(input, dimensions));
        }
        return vectors;
    }
}
