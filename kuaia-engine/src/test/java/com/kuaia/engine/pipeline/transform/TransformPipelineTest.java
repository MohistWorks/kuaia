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
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void trimTransformTrimsStringInPlaceAndPreservesSchema() throws Exception {
        TransformPipeline pipeline = TransformPipeline.from(
                rowType(),
                Collections.singletonList(trimConfig("content")));

        List<BinaryRow> outputs = pipeline.applyBatch(Arrays.asList(
                row(1L, "  Alpha  "),
                row(2L, "\tBeta\n")));

        assertArrayEquals(new String[]{"id", "content"}, pipeline.getOutputType().getFieldNames());
        assertArrayEquals(new DataType[]{DataType.LONG, DataType.STRING}, pipeline.getOutputType().getFieldTypes());
        assertEquals(2, outputs.size());
        assertEquals(1L, outputs.get(0).getLong(0));
        assertEquals("Alpha", outputs.get(0).getString(1));
        assertEquals(2L, outputs.get(1).getLong(0));
        assertEquals("Beta", outputs.get(1).getString(1));
    }

    @Test
    void trimTransformRejectsUnknownField() {
        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> TransformPipeline.from(rowType(), Collections.singletonList(trimConfig("missing"))));

        assertEquals("Unknown transform field: missing", error.getMessage());
    }

    @Test
    void trimTransformRejectsNonStringField() {
        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> TransformPipeline.from(rowType(), Collections.singletonList(trimConfig("id"))));

        assertEquals("Transform field must be STRING: id", error.getMessage());
    }

    @Test
    void filterTransformDropsEmptyAndWhitespaceStrings() throws Exception {
        TransformPipeline pipeline = TransformPipeline.from(
                rowType(),
                Collections.singletonList(filterConfig("content", "not-empty")));

        List<BinaryRow> outputs = pipeline.applyBatch(Arrays.asList(
                row(1L, "Alpha"),
                row(2L, ""),
                row(3L, "   "),
                row(4L, "Beta")));

        assertArrayEquals(new String[]{"id", "content"}, pipeline.getOutputType().getFieldNames());
        assertArrayEquals(new DataType[]{DataType.LONG, DataType.STRING}, pipeline.getOutputType().getFieldTypes());
        assertEquals(2, outputs.size());
        assertEquals(1L, outputs.get(0).getLong(0));
        assertEquals("Alpha", outputs.get(0).getString(1));
        assertEquals(4L, outputs.get(1).getLong(0));
        assertEquals("Beta", outputs.get(1).getString(1));
    }

    @Test
    void filterTransformDropsStringsShorterThanMinLength() throws Exception {
        TransformPipeline pipeline = TransformPipeline.from(
                rowType(),
                Collections.singletonList(filterConfig("content", "min-length", 5)));

        List<BinaryRow> outputs = pipeline.applyBatch(Arrays.asList(
                row(1L, "Alpha"),
                row(2L, "Beta"),
                row(3L, "  Gamma  "),
                row(4L, "   ")));

        assertArrayEquals(new String[]{"id", "content"}, pipeline.getOutputType().getFieldNames());
        assertArrayEquals(new DataType[]{DataType.LONG, DataType.STRING}, pipeline.getOutputType().getFieldTypes());
        assertEquals(2, outputs.size());
        assertEquals(1L, outputs.get(0).getLong(0));
        assertEquals("Alpha", outputs.get(0).getString(1));
        assertEquals(3L, outputs.get(1).getLong(0));
        assertEquals("  Gamma  ", outputs.get(1).getString(1));
    }

    @Test
    void filterTransformRejectsUnknownField() {
        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> TransformPipeline.from(rowType(), Collections.singletonList(filterConfig("missing", "not-empty"))));

        assertEquals("Unknown transform field: missing", error.getMessage());
    }

    @Test
    void filterTransformRejectsNonStringField() {
        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> TransformPipeline.from(rowType(), Collections.singletonList(filterConfig("id", "not-empty"))));

        assertEquals("Transform field must be STRING: id", error.getMessage());
    }

    @Test
    void chunkTransformExpandsRowsAndPreservesInputFields() throws Exception {
        TransformPipeline pipeline = TransformPipeline.from(
                rowType(),
                Collections.singletonList(chunkConfig(5, 1)));

        List<BinaryRow> outputs = pipeline.applyBatch(Collections.singletonList(row(7L, "abcdefghij")));

        assertArrayEquals(
                new String[]{"id", "content", "chunk", "chunk_index"},
                pipeline.getOutputType().getFieldNames());
        assertArrayEquals(
                new DataType[]{DataType.LONG, DataType.STRING, DataType.STRING, DataType.LONG},
                pipeline.getOutputType().getFieldTypes());
        assertEquals(3, outputs.size());
        assertEquals(7L, outputs.get(0).getLong(0));
        assertEquals("abcdefghij", outputs.get(0).getString(1));
        assertEquals("abcde", outputs.get(0).getString(2));
        assertEquals(0L, outputs.get(0).getLong(3));
        assertEquals("efghi", outputs.get(1).getString(2));
        assertEquals(1L, outputs.get(1).getLong(3));
        assertEquals("ij", outputs.get(2).getString(2));
        assertEquals(2L, outputs.get(2).getLong(3));
    }

    @Test
    void chunkTransformCanDropInputAndIncludeOffsets() throws Exception {
        TransformPipeline pipeline = TransformPipeline.from(
                rowType(),
                Collections.singletonList(chunkConfig("chunk", 5, 1, true, true)));

        List<BinaryRow> outputs = pipeline.applyBatch(Collections.singletonList(row(7L, "abcdefghij")));

        assertArrayEquals(
                new String[]{"id", "chunk", "chunk_index", "chunk_start", "chunk_end"},
                pipeline.getOutputType().getFieldNames());
        assertArrayEquals(
                new DataType[]{DataType.LONG, DataType.STRING, DataType.LONG, DataType.LONG, DataType.LONG},
                pipeline.getOutputType().getFieldTypes());
        assertEquals(3, outputs.size());
        assertEquals(7L, outputs.get(0).getLong(0));
        assertEquals("abcde", outputs.get(0).getString(1));
        assertEquals(0L, outputs.get(0).getLong(2));
        assertEquals(0L, outputs.get(0).getLong(3));
        assertEquals(5L, outputs.get(0).getLong(4));
        assertEquals("efghi", outputs.get(1).getString(1));
        assertEquals(1L, outputs.get(1).getLong(2));
        assertEquals(4L, outputs.get(1).getLong(3));
        assertEquals(9L, outputs.get(1).getLong(4));
        assertEquals("ij", outputs.get(2).getString(1));
        assertEquals(2L, outputs.get(2).getLong(2));
        assertEquals(8L, outputs.get(2).getLong(3));
        assertEquals(10L, outputs.get(2).getLong(4));
    }

    @Test
    void chunkTransformEmitsNoRowsForEmptyText() throws Exception {
        TransformPipeline pipeline = TransformPipeline.from(
                rowType(),
                Collections.singletonList(chunkConfig(5, 1)));

        List<BinaryRow> outputs = pipeline.applyBatch(Collections.singletonList(row(7L, "")));

        assertEquals(0, outputs.size());
    }

    @Test
    void chunkTransformRejectsUnknownInputField() {
        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> TransformPipeline.from(rowType(), Collections.singletonList(
                        new PipelineConfig.TransformConfig(
                                "chunk",
                                Collections.emptyList(),
                                null,
                                null,
                                "missing",
                                "chunk",
                                0,
                                null,
                                null,
                                null,
                                null,
                                30000,
                                32,
                                5,
                                1))));

        assertEquals("Unknown transform field: missing", error.getMessage());
    }

    @Test
    void chunkTransformRejectsNonStringInputField() {
        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> TransformPipeline.from(rowType(), Collections.singletonList(
                        new PipelineConfig.TransformConfig(
                                "chunk",
                                Collections.emptyList(),
                                null,
                                null,
                                "id",
                                "chunk",
                                0,
                                null,
                                null,
                                null,
                                null,
                                30000,
                                32,
                                5,
                                1))));

        assertEquals("Transform field must be STRING: id", error.getMessage());
    }

    @Test
    void chunkTransformRejectsDuplicateOutputField() {
        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> TransformPipeline.from(rowType(), Collections.singletonList(chunkConfig("content", 5, 1))));

        assertEquals("Duplicate transform field: content", error.getMessage());
    }

    @Test
    void chunkTransformRejectsExistingChunkIndexField() {
        KuaiaRowType inputType = new KuaiaRowType(
                new String[]{"id", "content", "chunk_index"},
                new DataType[]{DataType.LONG, DataType.STRING, DataType.LONG});

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> TransformPipeline.from(inputType, Collections.singletonList(chunkConfig(5, 1))));

        assertEquals("Duplicate transform field: chunk_index", error.getMessage());
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

    private PipelineConfig.TransformConfig trimConfig(String field) {
        return new PipelineConfig.TransformConfig(
                "trim",
                Collections.emptyList(),
                null,
                null,
                field,
                null,
                0);
    }

    private PipelineConfig.TransformConfig filterConfig(String field, String op) {
        return filterConfig(field, op, 0);
    }

    private PipelineConfig.TransformConfig filterConfig(String field, String op, int minLength) {
        return new PipelineConfig.TransformConfig(
                "filter",
                Collections.emptyList(),
                null,
                null,
                field,
                null,
                0,
                null,
                null,
                null,
                null,
                30000,
                32,
                0,
                0,
                false,
                false,
                op,
                minLength);
    }

    private PipelineConfig.TransformConfig chunkConfig(int chunkSize, int overlap) {
        return chunkConfig("chunk", chunkSize, overlap);
    }

    private PipelineConfig.TransformConfig chunkConfig(String output, int chunkSize, int overlap) {
        return chunkConfig(output, chunkSize, overlap, false, false);
    }

    private PipelineConfig.TransformConfig chunkConfig(
            String output,
            int chunkSize,
            int overlap,
            boolean dropInput,
            boolean includeOffsets) {
        return new PipelineConfig.TransformConfig(
                "chunk",
                Collections.emptyList(),
                null,
                null,
                "content",
                output,
                0,
                null,
                null,
                null,
                null,
                30000,
                32,
                chunkSize,
                overlap,
                dropInput,
                includeOffsets);
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
