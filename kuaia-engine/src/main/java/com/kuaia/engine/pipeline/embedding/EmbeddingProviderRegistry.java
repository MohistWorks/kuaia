package com.kuaia.engine.pipeline.embedding;

import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EmbeddingProviderRegistry {
    private final Map<String, EmbeddingProvider> providers;

    public EmbeddingProviderRegistry(Map<String, EmbeddingProvider> providers) {
        this.providers = Collections.unmodifiableMap(new HashMap<>(providers));
    }

    public static EmbeddingProviderRegistry defaultRegistry() {
        Map<String, EmbeddingProvider> providers = new HashMap<>();
        providers.put("mock", new MockEmbeddingProvider());
        return new EmbeddingProviderRegistry(providers);
    }

    public EmbeddingProvider get(String name) throws PipelineExecutionException {
        EmbeddingProvider provider = providers.get(name);
        if (provider == null) {
            throw new PipelineExecutionException("Unsupported embedding provider: " + name);
        }
        return provider;
    }

    public EmbeddingProvider create(PipelineConfig.TransformConfig config) throws PipelineExecutionException {
        if ("openai-compatible".equals(config.getProvider())) {
            return new OpenAICompatibleEmbeddingProvider(
                    config.getBaseUrl(),
                    config.getModel(),
                    config.getApiKeyEnv(),
                    config.getTimeoutMs());
        }
        return get(config.getProvider());
    }
}
