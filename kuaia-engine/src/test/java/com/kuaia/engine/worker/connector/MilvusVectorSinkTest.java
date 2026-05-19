package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MilvusVectorSinkTest {
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void writesBatchRowsToMilvusUpsertEndpoint() throws Exception {
        CapturedRequest captured = startServer(200, "{\"code\":0,\"data\":{\"upsertCount\":2}}");
        PipelineConfig.SinkConfig config = config(
                baseUrl(),
                "docs",
                "MILVUS_TOKEN",
                "id",
                "embedding",
                Collections.singletonList("content"),
                0);
        MilvusVectorSink sink = new MilvusVectorSink(rowType(), config, env("MILVUS_TOKEN", "root:Milvus"));

        sink.open();
        sink.writeBatch(Arrays.asList(
                row(7L, "Alpha", new float[]{1.0f, 2.0f}),
                row(8L, "Beta", new float[]{3.0f, 4.0f})));
        sink.close();

        assertEquals("POST", captured.method);
        assertEquals("/v2/vectordb/entities/upsert", captured.path);
        assertEquals("Bearer root:Milvus", captured.authorization);
        assertEquals(
                "{\"collectionName\":\"docs\",\"data\":["
                        + "{\"id\":7,\"embedding\":[1.0,2.0],\"content\":\"Alpha\"},"
                        + "{\"id\":8,\"embedding\":[3.0,4.0],\"content\":\"Beta\"}"
                        + "]}",
                captured.body);
    }

    @Test
    void defaultsPayloadFieldsToNonIdNonVectorFields() throws Exception {
        CapturedRequest captured = startServer(200, "{\"code\":0}");
        PipelineConfig.SinkConfig config = config(
                baseUrl(),
                "docs",
                null,
                "id",
                "embedding",
                Collections.emptyList(),
                0);
        MilvusVectorSink sink = new MilvusVectorSink(rowTypeWithSource(), config, Collections.emptyMap());

        sink.open();
        sink.write(rowWithSource());
        sink.close();

        assertEquals(
                "{\"collectionName\":\"docs\",\"data\":["
                        + "{\"id\":7,\"embedding\":[1.0,2.0],\"content\":\"Alpha\",\"source\":\"kb\"}"
                        + "]}",
                captured.body);
    }

    @Test
    void requiresConfiguredTokenEnvironmentVariable() {
        PipelineConfig.SinkConfig config = config(
                "http://localhost:19530",
                "docs",
                "MILVUS_TOKEN",
                "id",
                "embedding",
                Collections.singletonList("content"),
                0);

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new MilvusVectorSink(rowType(), config, Collections.emptyMap()));

        assertEquals("Missing Milvus token environment variable: MILVUS_TOKEN", error.getMessage());
    }

    @Test
    void rejectsIdFieldInPayloadFields() {
        PipelineConfig.SinkConfig config = config(
                "http://localhost:19530",
                "docs",
                null,
                "id",
                "embedding",
                Collections.singletonList("id"),
                0);

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new MilvusVectorSink(rowType(), config, Collections.emptyMap()));

        assertEquals("Milvus payload field must not be the id field: id", error.getMessage());
    }

    @Test
    void rejectsMilvusApplicationErrorResponse() throws Exception {
        startServer(200, "{\"code\":1100,\"message\":\"collection not found\"}");
        PipelineConfig.SinkConfig config = config(
                baseUrl(),
                "docs",
                null,
                "id",
                "embedding",
                Collections.singletonList("content"),
                0);
        MilvusVectorSink sink = new MilvusVectorSink(rowType(), config, Collections.emptyMap());

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, () -> sink.write(row()));

        assertEquals(
                "Milvus upsert returned code 1100: {\"code\":1100,\"message\":\"collection not found\"}",
                error.getMessage());
    }

    private CapturedRequest startServer(int status, String response) throws Exception {
        CapturedRequest captured = new CapturedRequest();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v2/vectordb/entities/upsert", exchange -> {
            captured.method = exchange.getRequestMethod();
            captured.path = exchange.getRequestURI().getPath();
            captured.authorization = exchange.getRequestHeaders().getFirst("Authorization");
            captured.body = read(exchange.getRequestBody());
            byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, responseBytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(responseBytes);
            }
        });
        server.start();
        return captured;
    }

    private String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private KuaiaRowType rowType() {
        return new KuaiaRowType(
                new String[]{"id", "content", "embedding"},
                new DataType[]{DataType.LONG, DataType.STRING, DataType.VECTOR});
    }

    private KuaiaRowType rowTypeWithSource() {
        return new KuaiaRowType(
                new String[]{"id", "content", "source", "embedding"},
                new DataType[]{DataType.LONG, DataType.STRING, DataType.STRING, DataType.VECTOR});
    }

    private BinaryRow row() {
        return row(7L, "Alpha", new float[]{1.0f, 2.0f});
    }

    private BinaryRow row(long id, String content, float[] vector) {
        BinaryRow row = new BinaryRow(3);
        row.setLong(0, id);
        row.setString(1, content);
        row.setVector(2, vector);
        return row;
    }

    private BinaryRow rowWithSource() {
        BinaryRow row = new BinaryRow(4);
        row.setLong(0, 7L);
        row.setString(1, "Alpha");
        row.setString(2, "kb");
        row.setVector(3, new float[]{1.0f, 2.0f});
        return row;
    }

    private PipelineConfig.SinkConfig config(
            String url,
            String collection,
            String apiKeyEnv,
            String idField,
            String vectorField,
            java.util.List<String> payloadFields,
            int timeoutMs) {
        return new PipelineConfig.SinkConfig(
                "milvus",
                null,
                null,
                null,
                url,
                collection,
                apiKeyEnv,
                idField,
                vectorField,
                true,
                timeoutMs,
                null,
                0L,
                payloadFields);
    }

    private Map<String, String> env(String key, String value) {
        Map<String, String> env = new HashMap<>();
        env.put(key, value);
        return env;
    }

    private String read(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            bytes.write(buffer, 0, read);
        }
        return bytes.toString(StandardCharsets.UTF_8.name());
    }

    private static class CapturedRequest {
        private String method;
        private String path;
        private String authorization;
        private String body;
    }
}
