package com.kuaia.engine.pipeline.embedding;

public class MockEmbeddingProvider implements EmbeddingProvider {
    @Override
    public float[] embed(String input, int dimensions) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = input.length() + i;
        }
        return vector;
    }
}
