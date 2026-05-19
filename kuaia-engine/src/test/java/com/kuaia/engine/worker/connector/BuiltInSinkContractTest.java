package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BuiltInSinkContractTest {
    @TempDir
    Path tempDir;

    @Test
    void consoleSinkWritesEveryBatchRow() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ConsoleSink sink = new ConsoleSink(textRowType(), new PrintStream(bytes, true, StandardCharsets.UTF_8.name()));

        sink.open();
        sink.writeBatch(Arrays.asList(textRow(7L, "Alpha"), textRow(8L, "Beta")));
        sink.close();

        String output = bytes.toString(StandardCharsets.UTF_8.name());
        assertTrue(output.contains("[Kuaia] Row: id=7, content=Alpha"), output);
        assertTrue(output.contains("[Kuaia] Row: id=8, content=Beta"), output);
    }

    @Test
    void fileSinkWritesEveryBatchRow() throws Exception {
        Path output = tempDir.resolve("out/documents.csv");
        FileSink sink = new FileSink(textRowType(), output, "csv", "overwrite");

        sink.open();
        sink.writeBatch(Arrays.asList(textRow(7L, "Alpha"), textRow(8L, "Beta")));
        sink.close();

        assertEquals(
                Arrays.asList("id,content", "7,Alpha", "8,Beta"),
                Files.readAllLines(output, StandardCharsets.UTF_8));
    }

    @Test
    void mockVectorSinkWritesEveryBatchRow() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        MockVectorSink sink = new MockVectorSink(
                vectorRowType(),
                new PrintStream(bytes, true, StandardCharsets.UTF_8.name()));

        sink.open();
        sink.writeBatch(Arrays.asList(vectorRow(7L, "Alpha", 1.0f), vectorRow(8L, "Beta", 3.0f)));
        sink.close();

        String output = bytes.toString(StandardCharsets.UTF_8.name());
        assertTrue(output.contains("[AI Sink] Row ID: 7, Vector Dim: 2, First Val: 1.0000"), output);
        assertTrue(output.contains("[AI Sink] Row ID: 8, Vector Dim: 2, First Val: 3.0000"), output);
    }

    @Test
    void qdrantSinkWritesBatchInSingleUpsert() throws Exception {
        RecordingHttpFactory http = new RecordingHttpFactory(200, "{\"status\":\"ok\"}");
        QdrantVectorSink sink = new QdrantVectorSink(
                vectorRowType(),
                vectorHttpConfig("qdrant", "http://qdrant.local", "docs"),
                Collections.emptyMap(),
                http);

        sink.open();
        sink.writeBatch(Arrays.asList(vectorRow(7L, "Alpha", 1.0f), vectorRow(8L, "Beta", 3.0f)));
        sink.close();

        RecordingHttpConnection connection = http.onlyConnection();
        assertEquals("PUT", connection.getRequestMethod());
        assertEquals("http://qdrant.local/collections/docs/points?wait=true", connection.getURL().toString());
        assertEquals(
                "{\"points\":["
                        + "{\"id\":7,\"vector\":[1.0,2.0],\"payload\":{\"content\":\"Alpha\"}},"
                        + "{\"id\":8,\"vector\":[3.0,4.0],\"payload\":{\"content\":\"Beta\"}}"
                        + "]}",
                connection.requestBody());
    }

    @Test
    void pgvectorSinkWritesBatchInSingleUpsert() throws Exception {
        RecordingPgvectorConnectionFactory jdbc = new RecordingPgvectorConnectionFactory();
        PgvectorVectorSink sink = new PgvectorVectorSink(
                vectorRowType(),
                pgvectorConfig(),
                env("KUAIA_POSTGRES_USER", "kuaia", "KUAIA_POSTGRES_PASSWORD", "secret"),
                jdbc);

        sink.open();
        sink.writeBatch(Arrays.asList(vectorRow(7L, "Alpha", 1.0f), vectorRow(8L, "Beta", 3.0f)));
        sink.close();

        assertEquals("jdbc:postgresql://postgres.local:5432/kuaia", jdbc.url);
        assertEquals("kuaia", jdbc.properties.getProperty("user"));
        assertEquals("secret", jdbc.properties.getProperty("password"));
        assertEquals(
                "INSERT INTO document_vectors (id, embedding, content) VALUES (?, ?::vector, ?) "
                        + "ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding, "
                        + "content = EXCLUDED.content",
                jdbc.sql);
        assertEquals(Arrays.asList(7L, "[1.0,2.0]", "Alpha"), jdbc.batches.get(0));
        assertEquals(Arrays.asList(8L, "[3.0,4.0]", "Beta"), jdbc.batches.get(1));
        assertEquals(1, jdbc.closeCalls);
    }

    @Test
    void milvusSinkWritesBatchInSingleUpsert() throws Exception {
        RecordingHttpFactory http = new RecordingHttpFactory(200, "{\"code\":0}");
        MilvusVectorSink sink = new MilvusVectorSink(
                vectorRowType(),
                vectorHttpConfig("milvus", "http://milvus.local", "docs"),
                Collections.emptyMap(),
                http);

        sink.open();
        sink.writeBatch(Arrays.asList(vectorRow(7L, "Alpha", 1.0f), vectorRow(8L, "Beta", 3.0f)));
        sink.close();

        RecordingHttpConnection connection = http.onlyConnection();
        assertEquals("POST", connection.getRequestMethod());
        assertEquals("http://milvus.local/v2/vectordb/entities/upsert", connection.getURL().toString());
        assertEquals(
                "{\"collectionName\":\"docs\",\"data\":["
                        + "{\"id\":7,\"embedding\":[1.0,2.0],\"content\":\"Alpha\"},"
                        + "{\"id\":8,\"embedding\":[3.0,4.0],\"content\":\"Beta\"}"
                        + "]}",
                connection.requestBody());
    }

    private KuaiaRowType textRowType() {
        return new KuaiaRowType(
                new String[]{"id", "content"},
                new DataType[]{DataType.LONG, DataType.STRING});
    }

    private KuaiaRowType vectorRowType() {
        return new KuaiaRowType(
                new String[]{"id", "content", "embedding"},
                new DataType[]{DataType.LONG, DataType.STRING, DataType.VECTOR});
    }

    private BinaryRow textRow(long id, String content) {
        BinaryRow row = new BinaryRow(2);
        row.setLong(0, id);
        row.setString(1, content);
        return row;
    }

    private BinaryRow vectorRow(long id, String content, float first) {
        BinaryRow row = new BinaryRow(3);
        row.setLong(0, id);
        row.setString(1, content);
        row.setVector(2, new float[]{first, first + 1.0f});
        return row;
    }

    private PipelineConfig.SinkConfig vectorHttpConfig(String type, String url, String collection) {
        return new PipelineConfig.SinkConfig(
                type,
                null,
                null,
                null,
                url,
                collection,
                null,
                "id",
                "embedding",
                true,
                0,
                null,
                0L,
                Collections.singletonList("content"));
    }

    private PipelineConfig.SinkConfig pgvectorConfig() {
        return new PipelineConfig.SinkConfig(
                "pgvector",
                null,
                null,
                null,
                "jdbc:postgresql://postgres.local:5432/kuaia",
                null,
                null,
                "id",
                "embedding",
                true,
                0,
                null,
                0L,
                Collections.singletonList("content"),
                "document_vectors",
                "KUAIA_POSTGRES_USER",
                "KUAIA_POSTGRES_PASSWORD");
    }

    private Map<String, String> env(String... keyValues) {
        Map<String, String> env = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            env.put(keyValues[i], keyValues[i + 1]);
        }
        return env;
    }

    private static final class RecordingHttpFactory
            implements QdrantVectorSink.ConnectionFactory, MilvusVectorSink.ConnectionFactory {
        private final int status;
        private final String response;
        private final List<RecordingHttpConnection> connections = new ArrayList<>();

        private RecordingHttpFactory(int status, String response) {
            this.status = status;
            this.response = response;
        }

        @Override
        public HttpURLConnection open(URL url) {
            RecordingHttpConnection connection = new RecordingHttpConnection(url, status, response);
            connections.add(connection);
            return connection;
        }

        private RecordingHttpConnection onlyConnection() {
            assertEquals(1, connections.size());
            return connections.get(0);
        }
    }

    private static final class RecordingHttpConnection extends HttpURLConnection {
        private final int status;
        private final byte[] response;
        private final ByteArrayOutputStream requestBody = new ByteArrayOutputStream();

        private RecordingHttpConnection(URL url, int status, String response) {
            super(url);
            this.status = status;
            this.response = response.getBytes(StandardCharsets.UTF_8);
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
            return status;
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(response);
        }

        @Override
        public InputStream getErrorStream() {
            if (status >= 200 && status < 300) {
                return null;
            }
            return new ByteArrayInputStream(response);
        }

        private String requestBody() {
            return new String(requestBody.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private static final class RecordingPgvectorConnectionFactory implements PgvectorVectorSink.ConnectionFactory {
        private String url;
        private Properties properties;
        private String sql;
        private int closeCalls;
        private final List<List<Object>> batches = new ArrayList<>();

        @Override
        public Connection open(String url, Properties properties) {
            this.url = url;
            this.properties = properties;
            InvocationHandler handler = (proxy, method, args) -> {
                if ("prepareStatement".equals(method.getName())) {
                    sql = (String) args[0];
                    return preparedStatement();
                }
                if ("close".equals(method.getName())) {
                    closeCalls++;
                    return null;
                }
                return defaultValue(method);
            };
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    handler);
        }

        private PreparedStatement preparedStatement() {
            InvocationHandler handler = new InvocationHandler() {
                private final List<Object> params = new ArrayList<>();

                @Override
                public Object invoke(Object proxy, Method method, Object[] args) {
                    if ("setLong".equals(method.getName()) || "setString".equals(method.getName())) {
                        setParam((Integer) args[0], args[1]);
                        return null;
                    }
                    if ("addBatch".equals(method.getName())) {
                        batches.add(new ArrayList<>(params));
                        return null;
                    }
                    if ("executeBatch".equals(method.getName())) {
                        int[] result = new int[batches.size()];
                        Arrays.fill(result, 1);
                        return result;
                    }
                    return defaultValue(method);
                }

                private void setParam(int index, Object value) {
                    while (params.size() < index) {
                        params.add(null);
                    }
                    params.set(index - 1, value);
                }
            };
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class<?>[]{PreparedStatement.class},
                    handler);
        }

        private static Object defaultValue(Method method) {
            Class<?> returnType = method.getReturnType();
            if (returnType == Boolean.TYPE) {
                return false;
            }
            if (returnType == Integer.TYPE) {
                return 0;
            }
            if (returnType == Long.TYPE) {
                return 0L;
            }
            return null;
        }
    }
}
