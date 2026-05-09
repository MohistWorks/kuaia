package com.kuaia.engine.pipeline.embedding;

import com.kuaia.engine.pipeline.PipelineExecutionException;

public interface EmbeddingProvider {
    float[] embed(String input, int dimensions) throws PipelineExecutionException;
}
