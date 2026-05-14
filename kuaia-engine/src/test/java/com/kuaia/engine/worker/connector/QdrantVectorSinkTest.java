package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QdrantVectorSinkTest {
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void writesPointToQdrant() throws Exception {
        CapturedRequest captured = startServer(200, "{\"status\":\"ok\"}");
        PipelineConfig.SinkConfig config = config(baseUrl(), "docs", "QDRANT_API_KEY", "id", "embedding", true);
        QdrantVectorSink sink = new QdrantVectorSink(rowType(), config, env("QDRANT_API_KEY", "secret-token"));

        sink.open();
        sink.write(row());
        sink.close();

        assertEquals("PUT", captured.method);
        assertEquals("/collections/docs/points", captured.path);
        assertEquals("wait=true", captured.query);
        assertEquals("secret-token", captured.apiKey);
        assertEquals(
                "{\"points\":[{\"id\":7,\"vector\":[1.0,2.0],\"payload\":{\"id\":7,\"content\":\"Alpha\"}}]}",
                captured.body);
    }

    @Test
    void writesBatchPointsToQdrantInSingleRequest() throws Exception {
        CapturedRequest captured = startServer(200, "{\"status\":\"ok\"}");
        PipelineConfig.SinkConfig config = config(baseUrl(), "docs", null, "id", "embedding", true);
        QdrantVectorSink sink = new QdrantVectorSink(rowType(), config, Collections.emptyMap());

        sink.open();
        sink.writeBatch(Arrays.asList(
                row(7L, "Alpha", new float[]{1.0f, 2.0f}),
                row(8L, "Beta", new float[]{3.0f, 4.0f})));
        sink.close();

        assertEquals("PUT", captured.method);
        assertEquals(
                "{\"points\":["
                        + "{\"id\":7,\"vector\":[1.0,2.0],\"payload\":{\"id\":7,\"content\":\"Alpha\"}},"
                        + "{\"id\":8,\"vector\":[3.0,4.0],\"payload\":{\"id\":8,\"content\":\"Beta\"}}"
                        + "]}",
                captured.body);
    }

    @Test
    void writesChunkedRowsWithGeneratedPointIds() throws Exception {
        CapturedRequest captured = startServer(200, "{\"status\":\"ok\"}");
        PipelineConfig.SinkConfig config = configWithChunkIds(
                baseUrl(),
                "docs",
                "id",
                "embedding",
                "chunk_index",
                1000L);
        QdrantVectorSink sink = new QdrantVectorSink(chunkedRowType(), config, Collections.emptyMap());

        sink.open();
        sink.writeBatch(Arrays.asList(
                chunkedRow(7L, "Alpha chunk 0", 0L, new float[]{1.0f, 2.0f}),
                chunkedRow(7L, "Alpha chunk 1", 1L, new float[]{3.0f, 4.0f})));
        sink.close();

        assertEquals(
                "{\"points\":["
                        + "{\"id\":7000,\"vector\":[1.0,2.0],\"payload\":{\"id\":7,\"chunk\":\"Alpha chunk 0\",\"chunk_index\":0}},"
                        + "{\"id\":7001,\"vector\":[3.0,4.0],\"payload\":{\"id\":7,\"chunk\":\"Alpha chunk 1\",\"chunk_index\":1}}"
                        + "]}",
                captured.body);
    }

    @Test
    void writesOnlyConfiguredPayloadFieldsToQdrant() throws Exception {
        CapturedRequest captured = startServer(200, "{\"status\":\"ok\"}");
        PipelineConfig.SinkConfig config = configWithPayloadFields(
                baseUrl(),
                "docs",
                "id",
                "embedding",
                Arrays.asList("id", "source"));
        QdrantVectorSink sink = new QdrantVectorSink(rowTypeWithSource(), config, Collections.emptyMap());

        sink.open();
        sink.write(rowWithSource());
        sink.close();

        assertEquals(
                "{\"points\":[{\"id\":7,\"vector\":[1.0,2.0],\"payload\":{\"id\":7,\"source\":\"kb\"}}]}",
                captured.body);
    }

    @Test
    void rejectsMissingConfiguredPayloadField() {
        PipelineConfig.SinkConfig config = configWithPayloadFields(
                "http://localhost:6333",
                "docs",
                "id",
                "embedding",
                Collections.singletonList("missing"));

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new QdrantVectorSink(rowType(), config, Collections.emptyMap()));

        assertEquals("Qdrant sink requires payload field: missing", error.getMessage());
    }

    @Test
    void rejectsVectorConfiguredPayloadField() {
        PipelineConfig.SinkConfig config = configWithPayloadFields(
                "http://localhost:6333",
                "docs",
                "id",
                "embedding",
                Collections.singletonList("embedding"));

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new QdrantVectorSink(rowType(), config, Collections.emptyMap()));

        assertEquals("Qdrant payload field must not be the vector field: embedding", error.getMessage());
    }

    @Test
    void requiresLongChunkIndexField() {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "chunk_index", "embedding"},
                new DataType[]{DataType.LONG, DataType.STRING, DataType.VECTOR});
        PipelineConfig.SinkConfig config = configWithChunkIds(
                "http://localhost:6333",
                "docs",
                "id",
                "embedding",
                "chunk_index",
                1000L);

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new QdrantVectorSink(rowType, config, Collections.emptyMap()));

        assertEquals("Qdrant sink requires LONG field: chunk_index", error.getMessage());
    }

    @Test
    void rejectsGeneratedPointIdOverflow() throws Exception {
        PipelineConfig.SinkConfig config = configWithChunkIds(
                "http://localhost:6333",
                "docs",
                "id",
                "embedding",
                "chunk_index",
                2L);
        QdrantVectorSink sink = new QdrantVectorSink(chunkedRowType(), config, Collections.emptyMap());
        BinaryRow row = chunkedRow(Long.MAX_VALUE, "Alpha", 1L, new float[]{1.0f, 2.0f});

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, () -> sink.write(row));

        assertEquals("Qdrant generated point id overflow for id 9223372036854775807 and chunk index 1",
                error.getMessage());
    }

    @Test
    void appliesConfiguredTimeoutToUpsertConnection() throws Exception {
        CapturingConnection connection = new CapturingConnection();
        PipelineConfig.SinkConfig config = config(
                "http://localhost:6333",
                "docs",
                null,
                "id",
                "embedding",
                true,
                12000);
        QdrantVectorSink sink = new QdrantVectorSink(
                rowType(),
                config,
                Collections.emptyMap(),
                url -> connection);

        sink.open();
        sink.write(row());
        sink.close();

        assertEquals(12000, connection.getConnectTimeout());
        assertEquals(12000, connection.getReadTimeout());
    }

    @Test
    void omitsApiKeyHeaderWhenApiKeyEnvIsNotConfigured() throws Exception {
        CapturedRequest captured = startServer(200, "{\"status\":\"ok\"}");
        PipelineConfig.SinkConfig config = config(baseUrl(), "docs", null, "id", "embedding", false);
        QdrantVectorSink sink = new QdrantVectorSink(rowType(), config, Collections.emptyMap());

        sink.open();
        sink.write(row());
        sink.close();

        assertEquals("wait=false", captured.query);
        assertEquals(null, captured.apiKey);
    }

    @Test
    void rejectsMissingApiKeyEnvironmentVariable() {
        PipelineConfig.SinkConfig config = config(
                "http://localhost:6333",
                "docs",
                "MISSING_QDRANT_API_KEY",
                "id",
                "embedding",
                true);

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new QdrantVectorSink(rowType(), config, Collections.emptyMap()));

        assertEquals("Missing Qdrant API key environment variable: MISSING_QDRANT_API_KEY", error.getMessage());
    }

    @Test
    void rejectsNonSuccessResponse() throws Exception {
        startServer(404, "collection not found");
        PipelineConfig.SinkConfig config = config(baseUrl(), "docs", null, "id", "embedding", true);
        QdrantVectorSink sink = new QdrantVectorSink(rowType(), config, Collections.emptyMap());

        sink.open();
        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, () -> sink.write(row()));
        sink.close();

        assertEquals("Qdrant upsert failed with status 404: collection not found", error.getMessage());
    }

    @Test
    void rejectsQdrantApplicationErrorResponse() throws Exception {
        startServer(200, "{\"status\":\"error\",\"result\":null}");
        PipelineConfig.SinkConfig config = config(baseUrl(), "docs", null, "id", "embedding", true);
        QdrantVectorSink sink = new QdrantVectorSink(rowType(), config, Collections.emptyMap());

        sink.open();
        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, () -> sink.write(row()));
        sink.close();

        assertEquals(
                "Qdrant upsert returned status error: {\"status\":\"error\",\"result\":null}",
                error.getMessage());
    }

    @Test
    void wrapsQdrantIoFailures() throws Exception {
        PipelineConfig.SinkConfig config = config(
                "http://localhost:6333",
                "docs",
                null,
                "id",
                "embedding",
                true);
        QdrantVectorSink sink = new QdrantVectorSink(
                rowType(),
                config,
                Collections.emptyMap(),
                url -> {
                    throw new IOException("connect timed out");
                });

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, () -> sink.write(row()));

        assertEquals("Qdrant upsert failed: connect timed out", error.getMessage());
    }

    @Test
    void requiresLongIdField() {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"doc_id", "embedding"},
                new DataType[]{DataType.STRING, DataType.VECTOR});
        PipelineConfig.SinkConfig config = config("http://localhost:6333", "docs", null, "doc_id", "embedding", true);

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new QdrantVectorSink(rowType, config, Collections.emptyMap()));

        assertEquals("Qdrant sink requires LONG field: doc_id", error.getMessage());
    }

    @Test
    void requiresVectorField() {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "content"},
                new DataType[]{DataType.LONG, DataType.STRING});
        PipelineConfig.SinkConfig config = config("http://localhost:6333", "docs", null, "id", "embedding", true);

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new QdrantVectorSink(rowType, config, Collections.emptyMap()));

        assertEquals("Qdrant sink requires VECTOR field: embedding", error.getMessage());
    }

    private CapturedRequest startServer(int status, String response) throws Exception {
        CapturedRequest captured = new CapturedRequest();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/collections/docs/points", exchange -> {
            captured.method = exchange.getRequestMethod();
            captured.path = exchange.getRequestURI().getPath();
            captured.query = exchange.getRequestURI().getQuery();
            captured.apiKey = exchange.getRequestHeaders().getFirst("api-key");
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

    private KuaiaRowType chunkedRowType() {
        return new KuaiaRowType(
                new String[]{"id", "chunk", "chunk_index", "embedding"},
                new DataType[]{DataType.LONG, DataType.STRING, DataType.LONG, DataType.VECTOR});
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

    private BinaryRow chunkedRow(long id, String chunk, long chunkIndex, float[] vector) {
        BinaryRow row = new BinaryRow(4);
        row.setLong(0, id);
        row.setString(1, chunk);
        row.setLong(2, chunkIndex);
        row.setVector(3, vector);
        return row;
    }

    private PipelineConfig.SinkConfig config(
            String url,
            String collection,
            String apiKeyEnv,
            String idField,
            String vectorField,
            boolean wait) {
        return config(url, collection, apiKeyEnv, idField, vectorField, wait, 0);
    }

    private PipelineConfig.SinkConfig config(
            String url,
            String collection,
            String apiKeyEnv,
            String idField,
            String vectorField,
            boolean wait,
            int timeoutMs) {
        return new PipelineConfig.SinkConfig(
                "qdrant",
                null,
                null,
                null,
                url,
                collection,
                apiKeyEnv,
                idField,
                vectorField,
                wait,
                timeoutMs);
    }

    private PipelineConfig.SinkConfig configWithChunkIds(
            String url,
            String collection,
            String idField,
            String vectorField,
            String chunkIndexField,
            long chunkIdMultiplier) {
        return new PipelineConfig.SinkConfig(
                "qdrant",
                null,
                null,
                null,
                url,
                collection,
                null,
                idField,
                vectorField,
                true,
                0,
                chunkIndexField,
                chunkIdMultiplier);
    }

    private PipelineConfig.SinkConfig configWithPayloadFields(
            String url,
            String collection,
            String idField,
            String vectorField,
            java.util.List<String> payloadFields) {
        return new PipelineConfig.SinkConfig(
                "qdrant",
                null,
                null,
                null,
                url,
                collection,
                null,
                idField,
                vectorField,
                true,
                0,
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
        private String query;
        private String apiKey;
        private String body;
    }

    private static class CapturingConnection extends HttpURLConnection {
        private final ByteArrayOutputStream requestBody = new ByteArrayOutputStream();

        CapturingConnection() throws Exception {
            super(new URL("http://localhost:6333/collections/docs/points"));
        }

        @Override
        public void disconnect() {}

        @Override
        public boolean usingProxy() {
            return false;
        }

        @Override
        public void connect() {}

        @Override
        public OutputStream getOutputStream() {
            return requestBody;
        }

        @Override
        public int getResponseCode() {
            return 200;
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream("{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8));
        }
    }
}
