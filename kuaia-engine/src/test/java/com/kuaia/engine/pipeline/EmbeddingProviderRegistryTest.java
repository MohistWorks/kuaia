package com.kuaia.engine.pipeline;

import com.kuaia.engine.pipeline.embedding.EmbeddingProvider;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class EmbeddingProviderRegistryTest {
    @Test
    void defaultRegistryProvidesDeterministicMockEmbeddings() throws Exception {
        EmbeddingProvider provider = EmbeddingProviderRegistry.defaultRegistry().get("mock");

        assertArrayEquals(new float[]{5.0f, 6.0f, 7.0f, 8.0f}, provider.embed("Alpha", 4));
    }
}
