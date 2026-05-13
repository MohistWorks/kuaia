package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class QdrantVectorSink implements SinkWriter {
    private static final int DEFAULT_TIMEOUT_MILLIS = 30_000;

    private final KuaiaRowType rowType;
    private final int idOrdinal;
    private final int vectorOrdinal;
    private final int chunkIndexOrdinal;
    private final long chunkIdMultiplier;
    private final String upsertUrl;
    private final String apiKey;
    private final int timeoutMillis;
    private final ConnectionFactory connectionFactory;

    public QdrantVectorSink(KuaiaRowType rowType, PipelineConfig.SinkConfig config)
            throws PipelineExecutionException {
        this(rowType, config, System.getenv());
    }

    QdrantVectorSink(KuaiaRowType rowType, PipelineConfig.SinkConfig config, Map<String, String> environment)
            throws PipelineExecutionException {
        this(rowType, config, environment, QdrantVectorSink::openHttpConnection);
    }

    QdrantVectorSink(
            KuaiaRowType rowType,
            PipelineConfig.SinkConfig config,
            Map<String, String> environment,
            ConnectionFactory connectionFactory)
            throws PipelineExecutionException {
        this.rowType = rowType;
        this.idOrdinal = requireField(rowType, config.getIdField(), DataType.LONG);
        this.vectorOrdinal = requireField(rowType, config.getVectorField(), DataType.VECTOR);
        this.chunkIndexOrdinal = config.getChunkIndexField() == null
                ? -1
                : requireField(rowType, config.getChunkIndexField(), DataType.LONG);
        this.chunkIdMultiplier = config.getChunkIdMultiplier();
        this.upsertUrl = buildUpsertUrl(config);
        this.apiKey = loadApiKey(config.getApiKeyEnv(), environment);
        this.timeoutMillis = config.getTimeoutMs() > 0 ? config.getTimeoutMs() : DEFAULT_TIMEOUT_MILLIS;
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public void write(BinaryRow row) throws Exception {
        writeBatch(Collections.singletonList(row));
    }

    @Override
    public void writeBatch(List<BinaryRow> rows) throws Exception {
        if (rows.isEmpty()) {
            return;
        }
        byte[] body = buildRequestBody(rows).getBytes(StandardCharsets.UTF_8);
        try {
            HttpURLConnection connection = connectionFactory.open(new URL(upsertUrl));
            connection.setConnectTimeout(timeoutMillis);
            connection.setReadTimeout(timeoutMillis);
            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            if (apiKey != null) {
                connection.setRequestProperty("api-key", apiKey);
            }
            try (OutputStream out = connection.getOutputStream()) {
                out.write(body);
            }

            int status = connection.getResponseCode();
            if (status < 200 || status >= 300) {
                throw new PipelineExecutionException(
                        "Qdrant upsert failed with status " + status + ": " + readResponse(connection));
            }
            readResponse(connection);
        } catch (PipelineExecutionException e) {
            throw e;
        } catch (IOException e) {
            throw new PipelineExecutionException("Qdrant upsert failed: " + e.getMessage(), e);
        }
    }

    private String buildUpsertUrl(PipelineConfig.SinkConfig config) throws PipelineExecutionException {
        String baseUrl = config.getUrl();
        if (baseUrl == null || baseUrl.trim().isEmpty()) {
            throw new PipelineExecutionException("Missing required field: sink.url");
        }
        String trimmed = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        try {
            return trimmed
                    + "/collections/"
                    + URLEncoder.encode(config.getCollection(), StandardCharsets.UTF_8.name())
                    + "/points?wait="
                    + config.isWait();
        } catch (Exception e) {
            throw new PipelineExecutionException("Invalid qdrant collection: " + config.getCollection(), e);
        }
    }

    private String loadApiKey(String apiKeyEnv, Map<String, String> environment) throws PipelineExecutionException {
        if (apiKeyEnv == null || apiKeyEnv.trim().isEmpty()) {
            return null;
        }
        String value = environment.get(apiKeyEnv);
        if (value == null || value.trim().isEmpty()) {
            throw new PipelineExecutionException("Missing Qdrant API key environment variable: " + apiKeyEnv);
        }
        return value;
    }

    private String buildRequestBody(List<BinaryRow> rows) throws PipelineExecutionException {
        StringBuilder json = new StringBuilder();
        json.append("{\"points\":[");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                json.append(",");
            }
            BinaryRow row = rows.get(i);
            json.append("{");
            json.append("\"id\":").append(pointId(row));
            json.append(",\"vector\":").append(vectorJson(row.getVector(vectorOrdinal)));
            json.append(",\"payload\":").append(payloadJson(row));
            json.append("}");
        }
        json.append("]}");
        return json.toString();
    }

    private long pointId(BinaryRow row) throws PipelineExecutionException {
        long id = row.getLong(idOrdinal);
        if (chunkIndexOrdinal < 0) {
            return id;
        }
        long chunkIndex = row.getLong(chunkIndexOrdinal);
        if (chunkIndex < 0L) {
            throw new PipelineExecutionException("Qdrant chunk index must not be negative: " + chunkIndex);
        }
        try {
            return Math.addExact(Math.multiplyExact(id, chunkIdMultiplier), chunkIndex);
        } catch (ArithmeticException e) {
            throw new PipelineExecutionException(
                    "Qdrant generated point id overflow for id " + id + " and chunk index " + chunkIndex,
                    e);
        }
    }

    private String vectorJson(float[] vector) {
        StringBuilder json = new StringBuilder();
        json.append("[");
        for (int i = 0; i < vector.length; i++) {
            if (i > 0) {
                json.append(",");
            }
            json.append(Float.toString(vector[i]));
        }
        json.append("]");
        return json.toString();
    }

    private String payloadJson(BinaryRow row) throws PipelineExecutionException {
        StringBuilder json = new StringBuilder();
        json.append("{");
        String[] names = rowType.getFieldNames();
        DataType[] types = rowType.getFieldTypes();
        boolean first = true;
        for (int i = 0; i < names.length; i++) {
            if (i == vectorOrdinal) {
                continue;
            }
            if (!first) {
                json.append(",");
            }
            json.append("\"").append(escape(names[i])).append("\":");
            appendPayloadValue(json, row, i, types[i]);
            first = false;
        }
        json.append("}");
        return json.toString();
    }

    private void appendPayloadValue(StringBuilder json, BinaryRow row, int ordinal, DataType type)
            throws PipelineExecutionException {
        if (type == DataType.LONG) {
            json.append(row.getLong(ordinal));
            return;
        }
        if (type == DataType.STRING) {
            json.append("\"").append(escape(row.getString(ordinal))).append("\"");
            return;
        }
        throw new PipelineExecutionException("Qdrant sink does not support payload field type: " + type.name());
    }

    private int requireField(KuaiaRowType rowType, String field, DataType type) throws PipelineExecutionException {
        int ordinal = rowType.getIndex(field);
        if (ordinal < 0 || rowType.getFieldTypes()[ordinal] != type) {
            throw new PipelineExecutionException("Qdrant sink requires " + type.name() + " field: " + field);
        }
        return ordinal;
    }

    private String readResponse(HttpURLConnection connection) throws IOException {
        InputStream stream = connection.getErrorStream();
        if (stream == null) {
            stream = connection.getInputStream();
        }
        if (stream == null) {
            return "";
        }
        try (InputStream input = stream) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                bytes.write(buffer, 0, read);
            }
            return bytes.toString(StandardCharsets.UTF_8.name());
        }
    }

    private String escape(String value) {
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    escaped.append(c);
            }
        }
        return escaped.toString();
    }

    private static HttpURLConnection openHttpConnection(URL url) throws IOException {
        return (HttpURLConnection) url.openConnection();
    }

    interface ConnectionFactory {
        HttpURLConnection open(URL url) throws IOException;
    }
}
