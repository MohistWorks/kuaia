package com.kuaia.engine.pipeline.transform;

import com.kuaia.engine.pipeline.embedding.EmbeddingProvider;

public class MockEmbeddingTransform extends EmbeddingTransform {
    public MockEmbeddingTransform(String inputField, String outputField, int dimensions, EmbeddingProvider provider) {
        super(inputField, outputField, dimensions, provider);
    }

    public MockEmbeddingTransform(
            String inputField,
            String outputField,
            int dimensions,
            EmbeddingProvider provider,
            int batchSize) {
        super(inputField, outputField, dimensions, provider, batchSize);
    }
}
