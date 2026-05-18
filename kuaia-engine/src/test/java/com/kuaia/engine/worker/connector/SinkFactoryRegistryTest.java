package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkFactoryRegistryTest {
    @Test
    void defaultRegistryCreatesMockVectorSink() throws Exception {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "embedding"},
                new DataType[]{DataType.LONG, DataType.VECTOR});
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        SinkWriter sink = SinkFactoryRegistry.defaultRegistry().create(
                "mock-vector",
                rowType,
                new PrintStream(bytes, true, StandardCharsets.UTF_8.name()));
        BinaryRow row = new BinaryRow(2);
        row.setLong(0, 7L);
        row.setVector(1, new float[]{1.0f, 2.0f});

        sink.open();
        sink.write(row);
        sink.close();

        assertTrue(bytes.toString(StandardCharsets.UTF_8.name())
                .contains("[AI Sink] Row ID: 7, Vector Dim: 2, First Val: 1.0000"));
    }

    @Test
    void defaultRegistryRejectsUnsupportedSinkType() {
        KuaiaRowType rowType = new KuaiaRowType(new String[]{"id"}, new DataType[]{DataType.LONG});

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> SinkFactoryRegistry.defaultRegistry().create("missing", rowType, System.out));

        assertEquals("Unsupported sink.type: missing", error.getMessage());
    }

    @Test
    void defaultRegistryCreatesQdrantSink() throws Exception {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "embedding"},
                new DataType[]{DataType.LONG, DataType.VECTOR});
        PipelineConfig.SinkConfig config = new PipelineConfig.SinkConfig(
                "qdrant",
                null,
                null,
                null,
                "http://localhost:6333",
                "docs",
                null,
                "id",
                "embedding",
                true);

        SinkWriter sink = SinkFactoryRegistry.defaultRegistry().create(
                "qdrant",
                rowType,
                System.out,
                config);

        assertTrue(sink instanceof QdrantVectorSink);
    }

    @Test
    void defaultRegistryCreatesPgvectorSink() throws Exception {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "content", "embedding"},
                new DataType[]{DataType.LONG, DataType.STRING, DataType.VECTOR});
        PipelineConfig.SinkConfig config = new PipelineConfig.SinkConfig(
                "pgvector",
                null,
                null,
                null,
                "jdbc:postgresql://localhost:5432/kuaia",
                null,
                null,
                "id",
                "embedding",
                true,
                0,
                null,
                0L,
                java.util.Collections.singletonList("content"),
                "document_vectors",
                "KUAIA_POSTGRES_USER",
                "KUAIA_POSTGRES_PASSWORD");

        SinkWriter sink = SinkFactoryRegistry.defaultRegistry().create(
                "pgvector",
                rowType,
                System.out,
                config);

        assertTrue(sink instanceof PgvectorVectorSink);
    }
}
