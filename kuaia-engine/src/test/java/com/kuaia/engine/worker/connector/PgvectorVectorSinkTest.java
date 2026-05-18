package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

class PgvectorVectorSinkTest {
    @Test
    void writesBatchRowsWithPgvectorUpsert() throws Exception {
        RecordingConnectionFactory connectionFactory = new RecordingConnectionFactory();
        PgvectorVectorSink sink = new PgvectorVectorSink(
                rowType(),
                config("document_vectors", "id", "embedding", Collections.singletonList("content"), 12000),
                env(),
                connectionFactory);

        sink.open();
        sink.writeBatch(Arrays.asList(
                row(7L, "Alpha", new float[]{1.0f, 2.0f}),
                row(8L, "Beta", new float[]{3.0f, 4.0f})));
        sink.close();

        assertEquals("jdbc:postgresql://localhost:5432/kuaia", connectionFactory.url);
        assertEquals("kuaia", connectionFactory.properties.getProperty("user"));
        assertEquals("secret", connectionFactory.properties.getProperty("password"));
        assertEquals("12", connectionFactory.properties.getProperty("connectTimeout"));
        assertEquals("12", connectionFactory.properties.getProperty("socketTimeout"));
        assertEquals(
                "INSERT INTO document_vectors (id, embedding, content) VALUES (?, ?::vector, ?) "
                        + "ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding, "
                        + "content = EXCLUDED.content",
                connectionFactory.sql);
        assertEquals(Arrays.asList(7L, "[1.0,2.0]", "Alpha"), connectionFactory.batches.get(0));
        assertEquals(Arrays.asList(8L, "[3.0,4.0]", "Beta"), connectionFactory.batches.get(1));
    }

    @Test
    void defaultsPayloadFieldsToNonIdNonVectorFields() throws Exception {
        RecordingConnectionFactory connectionFactory = new RecordingConnectionFactory();
        PgvectorVectorSink sink = new PgvectorVectorSink(
                rowTypeWithSource(),
                config("document_vectors", "id", "embedding", Collections.emptyList(), 0),
                env(),
                connectionFactory);

        sink.open();
        sink.write(rowWithSource());
        sink.close();

        assertEquals(
                "INSERT INTO document_vectors (id, embedding, content, source) VALUES (?, ?::vector, ?, ?) "
                        + "ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding, "
                        + "content = EXCLUDED.content, source = EXCLUDED.source",
                connectionFactory.sql);
        assertEquals(Arrays.asList(7L, "[1.0,2.0]", "Alpha", "kb"), connectionFactory.batches.get(0));
    }

    @Test
    void rejectsIdFieldInPayloadFields() {
        PipelineConfig.SinkConfig config = config(
                "document_vectors",
                "id",
                "embedding",
                Arrays.asList("id", "content"),
                0);

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new PgvectorVectorSink(rowType(), config, env(), new RecordingConnectionFactory()));

        assertEquals("Pgvector payload field must not be the id field: id", error.getMessage());
    }

    @Test
    void requiresConfiguredUserEnvironmentVariable() throws Exception {
        Map<String, String> environment = new HashMap<>();
        environment.put("KUAIA_POSTGRES_PASSWORD", "secret");

        PgvectorVectorSink sink = new PgvectorVectorSink(
                rowType(),
                config("document_vectors", "id", "embedding", Collections.singletonList("content"), 0),
                environment,
                new RecordingConnectionFactory());

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, sink::open);

        assertEquals("Missing pgvector environment variable: KUAIA_POSTGRES_USER", error.getMessage());
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
            String table,
            String idField,
            String vectorField,
            List<String> payloadFields,
            int timeoutMs) {
        return new PipelineConfig.SinkConfig(
                "pgvector",
                null,
                null,
                null,
                "jdbc:postgresql://localhost:5432/kuaia",
                null,
                null,
                idField,
                vectorField,
                true,
                timeoutMs,
                null,
                0L,
                payloadFields,
                table,
                "KUAIA_POSTGRES_USER",
                "KUAIA_POSTGRES_PASSWORD");
    }

    private Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("KUAIA_POSTGRES_USER", "kuaia");
        env.put("KUAIA_POSTGRES_PASSWORD", "secret");
        return env;
    }

    private static class RecordingConnectionFactory implements PgvectorVectorSink.ConnectionFactory {
        private String url;
        private Properties properties;
        private String sql;
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
                    String name = method.getName();
                    if ("setLong".equals(name) || "setString".equals(name)) {
                        setParam((Integer) args[0], args[1]);
                        return null;
                    }
                    if ("addBatch".equals(name)) {
                        batches.add(new ArrayList<>(params));
                        return null;
                    }
                    if ("executeBatch".equals(name)) {
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
            if (returnType == Float.TYPE) {
                return 0.0f;
            }
            if (returnType == Double.TYPE) {
                return 0.0d;
            }
            return null;
        }
    }
}
