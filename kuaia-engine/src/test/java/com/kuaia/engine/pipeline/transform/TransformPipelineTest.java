package com.kuaia.engine.pipeline.transform;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.pipeline.embedding.EmbeddingProvider;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TransformPipelineTest {
    @Test
    void appliesEmbeddingTransformAsBatch() throws Exception {
        CountingEmbeddingProvider provider = new CountingEmbeddingProvider();
        Map<String, EmbeddingProvider> providers = new HashMap<>();
        providers.put("mock", provider);
        TransformPipeline pipeline = TransformPipeline.from(
                rowType(),
                Collections.singletonList(embeddingConfig(2)),
                new EmbeddingProviderRegistry(providers));

        List<BinaryRow> outputs = pipeline.applyBatch(Arrays.asList(row(1L, "Alpha"), row(2L, "Beta")));

        assertEquals(1, provider.batchCalls);
        assertEquals(0, provider.singleCalls);
        assertEquals(Arrays.asList("Alpha", "Beta"), provider.lastInputs);
        assertEquals(2, outputs.size());
        assertArrayEquals(new float[]{5.0f, 2.0f}, outputs.get(0).getVector(2), 0.00001f);
        assertArrayEquals(new float[]{4.0f, 2.0f}, outputs.get(1).getVector(2), 0.00001f);
    }

    private PipelineConfig.TransformConfig embeddingConfig(int batchSize) {
        return new PipelineConfig.TransformConfig(
                "embedding",
                Collections.emptyList(),
                null,
                null,
                "content",
                "embedding",
                2,
                "mock",
                null,
                null,
                null,
                30000,
                batchSize);
    }

    private KuaiaRowType rowType() {
        return new KuaiaRowType(
                new String[]{"id", "content"},
                new DataType[]{DataType.LONG, DataType.STRING});
    }

    private BinaryRow row(long id, String content) {
        BinaryRow row = new BinaryRow(2);
        row.setLong(0, id);
        row.setString(1, content);
        return row;
    }

    private static class CountingEmbeddingProvider implements EmbeddingProvider {
        private int singleCalls;
        private int batchCalls;
        private List<String> lastInputs;

        @Override
        public float[] embed(String input, int dimensions) {
            singleCalls++;
            return new float[]{input.length(), dimensions};
        }

        @Override
        public List<float[]> embedBatch(List<String> inputs, int dimensions) throws PipelineExecutionException {
            batchCalls++;
            lastInputs = new ArrayList<>(inputs);
            List<float[]> vectors = new ArrayList<>();
            for (String input : inputs) {
                vectors.add(new float[]{input.length(), dimensions});
            }
            return vectors;
        }
    }
}
