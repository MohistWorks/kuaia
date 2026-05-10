package com.kuaia.engine.pipeline;

import com.kuaia.engine.pipeline.embedding.EmbeddingProvider;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import com.kuaia.engine.pipeline.embedding.OpenAICompatibleEmbeddingProvider;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class EmbeddingProviderRegistryTest {
    @Test
    void defaultRegistryProvidesDeterministicMockEmbeddings() throws Exception {
        EmbeddingProvider provider = EmbeddingProviderRegistry.defaultRegistry().get("mock");

        assertArrayEquals(new float[]{5.0f, 6.0f, 7.0f, 8.0f}, provider.embed("Alpha", 4));
    }

    @Test
    void defaultRegistryCreatesOpenAICompatibleProviderFromTransformConfig() throws Exception {
        PipelineConfig.TransformConfig config = new PipelineConfig.TransformConfig(
                "embedding",
                Collections.emptyList(),
                null,
                null,
                "content",
                "embedding",
                0,
                "openai-compatible",
                "https://api.openai.com/v1",
                "text-embedding-3-small",
                "OPENAI_API_KEY");

        EmbeddingProvider provider = EmbeddingProviderRegistry.defaultRegistry().create(config);

        assertTrue(provider instanceof OpenAICompatibleEmbeddingProvider);
    }

    @Test
    void defaultRegistryPassesConfiguredOpenAICompatibleTimeout() throws Exception {
        PipelineConfig.TransformConfig config = openAICompatibleConfigWithTimeout(7500);

        EmbeddingProvider provider = EmbeddingProviderRegistry.defaultRegistry().create(config);

        assertEquals(7500, timeoutMillis(provider));
    }

    private PipelineConfig.TransformConfig openAICompatibleConfigWithTimeout(int timeoutMs) throws Exception {
        return new PipelineConfig.TransformConfig(
                "embedding",
                Collections.emptyList(),
                null,
                null,
                "content",
                "embedding",
                0,
                "openai-compatible",
                "https://api.openai.com/v1",
                "text-embedding-3-small",
                "OPENAI_API_KEY",
                timeoutMs);
    }

    private int timeoutMillis(EmbeddingProvider provider) throws Exception {
        try {
            Method method = provider.getClass().getDeclaredMethod("getTimeoutMillis");
            method.setAccessible(true);
            return (Integer) method.invoke(provider);
        } catch (NoSuchMethodException e) {
            fail("OpenAICompatibleEmbeddingProvider should expose its timeout for tests");
            return -1;
        } catch (InvocationTargetException e) {
            throw new AssertionError(e.getCause());
        }
    }
}
