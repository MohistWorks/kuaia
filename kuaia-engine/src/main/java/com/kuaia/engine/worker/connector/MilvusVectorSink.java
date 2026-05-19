package com.kuaia.engine.worker.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MilvusVectorSink implements SinkWriter {
    private static final int DEFAULT_TIMEOUT_MILLIS = 30_000;
    private static final int MAX_RESPONSE_CHARS = 500;
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final KuaiaRowType rowType;
    private final int idOrdinal;
    private final int vectorOrdinal;
    private final int[] payloadOrdinals;
    private final String collection;
    private final String upsertUrl;
    private final String token;
    private final int timeoutMillis;
    private final ConnectionFactory connectionFactory;

    public MilvusVectorSink(KuaiaRowType rowType, PipelineConfig.SinkConfig config)
            throws PipelineExecutionException {
        this(rowType, config, System.getenv());
    }

    MilvusVectorSink(KuaiaRowType rowType, PipelineConfig.SinkConfig config, Map<String, String> environment)
            throws PipelineExecutionException {
        this(rowType, config, environment, MilvusVectorSink::openHttpConnection);
    }

    MilvusVectorSink(
            KuaiaRowType rowType,
            PipelineConfig.SinkConfig config,
            Map<String, String> environment,
            ConnectionFactory connectionFactory)
            throws PipelineExecutionException {
        if (config == null) {
            throw new PipelineExecutionException("Missing milvus sink config");
        }
        this.rowType = rowType;
        this.idOrdinal = requireField(rowType, config.getIdField(), DataType.LONG);
        this.vectorOrdinal = requireField(rowType, config.getVectorField(), DataType.VECTOR);
        this.payloadOrdinals = resolvePayloadOrdinals(
                rowType,
                config.getPayloadFields(),
                idOrdinal,
                vectorOrdinal);
        this.collection = requireValue(config.getCollection(), "sink.collection");
        this.upsertUrl = buildUpsertUrl(config);
        this.token = loadToken(config.getApiKeyEnv(), environment);
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
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            if (token != null) {
                connection.setRequestProperty("Authorization", "Bearer " + token);
            }
            try (OutputStream out = connection.getOutputStream()) {
                out.write(body);
            }

            int status = connection.getResponseCode();
            if (status < 200 || status >= 300) {
                throw new PipelineExecutionException(
                        "Milvus upsert failed with status " + status + ": " + readResponse(connection));
            }
            validateSuccessResponse(readResponse(connection));
        } catch (PipelineExecutionException e) {
            throw e;
        } catch (IOException e) {
            throw new PipelineExecutionException("Milvus upsert failed: " + e.getMessage(), e);
        }
    }

    private String buildUpsertUrl(PipelineConfig.SinkConfig config) throws PipelineExecutionException {
        String baseUrl = requireValue(config.getUrl(), "sink.url");
        String trimmed = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        return trimmed + "/v2/vectordb/entities/upsert";
    }

    private String loadToken(String tokenEnv, Map<String, String> environment) throws PipelineExecutionException {
        if (tokenEnv == null || tokenEnv.trim().isEmpty()) {
            return null;
        }
        String value = environment.get(tokenEnv);
        if (value == null || value.trim().isEmpty()) {
            throw new PipelineExecutionException("Missing Milvus token environment variable: " + tokenEnv);
        }
        return value;
    }

    private String buildRequestBody(List<BinaryRow> rows) throws PipelineExecutionException {
        StringBuilder json = new StringBuilder();
        json.append("{\"collectionName\":\"").append(escape(collection)).append("\",\"data\":[");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                json.append(",");
            }
            appendRow(json, rows.get(i));
        }
        json.append("]}");
        return json.toString();
    }

    private void appendRow(StringBuilder json, BinaryRow row) throws PipelineExecutionException {
        String[] names = rowType.getFieldNames();
        json.append("{");
        json.append("\"").append(escape(names[idOrdinal])).append("\":").append(row.getLong(idOrdinal));
        json.append(",\"").append(escape(names[vectorOrdinal])).append("\":")
                .append(vectorJson(row.getVector(vectorOrdinal)));
        DataType[] types = rowType.getFieldTypes();
        for (int ordinal : payloadOrdinals) {
            json.append(",\"").append(escape(names[ordinal])).append("\":");
            appendPayloadValue(json, row, ordinal, types[ordinal]);
        }
        json.append("}");
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
        throw new PipelineExecutionException("Milvus sink does not support payload field type: " + type.name());
    }

    private void validateSuccessResponse(String response) throws PipelineExecutionException {
        String body = response == null ? "" : response.trim();
        if (body.isEmpty()) {
            throw new PipelineExecutionException("Milvus upsert returned empty response");
        }

        JsonNode root;
        try {
            root = JSON_MAPPER.readTree(body);
        } catch (IOException e) {
            throw new PipelineExecutionException("Milvus upsert returned invalid response: " + truncate(body), e);
        }

        JsonNode code = root.get("code");
        if (code == null || !code.canConvertToInt()) {
            throw new PipelineExecutionException("Milvus upsert returned invalid response: " + truncate(body));
        }
        if (code.asInt() != 0) {
            throw new PipelineExecutionException(
                    "Milvus upsert returned code " + code.asInt() + ": " + truncate(body));
        }
    }

    private int requireField(KuaiaRowType rowType, String field, DataType type) throws PipelineExecutionException {
        int ordinal = rowType.getIndex(field);
        if (ordinal < 0 || rowType.getFieldTypes()[ordinal] != type) {
            throw new PipelineExecutionException("Milvus sink requires " + type.name() + " field: " + field);
        }
        return ordinal;
    }

    private int[] resolvePayloadOrdinals(
            KuaiaRowType rowType,
            List<String> payloadFields,
            int idOrdinal,
            int vectorOrdinal) throws PipelineExecutionException {
        String[] names = rowType.getFieldNames();
        DataType[] types = rowType.getFieldTypes();
        if (payloadFields == null || payloadFields.isEmpty()) {
            List<Integer> ordinals = new ArrayList<>();
            for (int i = 0; i < names.length; i++) {
                if (i != idOrdinal && i != vectorOrdinal) {
                    validatePayloadField(types[i]);
                    ordinals.add(i);
                }
            }
            return toIntArray(ordinals);
        }

        List<Integer> ordinals = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String field : payloadFields) {
            if (!seen.add(field)) {
                throw new PipelineExecutionException("Duplicate Milvus payload field: " + field);
            }
            int ordinal = rowType.getIndex(field);
            if (ordinal < 0) {
                throw new PipelineExecutionException("Milvus sink requires payload field: " + field);
            }
            if (ordinal == idOrdinal) {
                throw new PipelineExecutionException("Milvus payload field must not be the id field: " + field);
            }
            if (ordinal == vectorOrdinal) {
                throw new PipelineExecutionException("Milvus payload field must not be the vector field: " + field);
            }
            validatePayloadField(types[ordinal]);
            ordinals.add(ordinal);
        }
        return toIntArray(ordinals);
    }

    private void validatePayloadField(DataType type) throws PipelineExecutionException {
        if (type != DataType.LONG && type != DataType.STRING) {
            throw new PipelineExecutionException(
                    "Milvus sink does not support payload field type: " + type.name());
        }
    }

    private int[] toIntArray(List<Integer> values) {
        int[] result = new int[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = values.get(i);
        }
        return result;
    }

    private String requireValue(String value, String field) throws PipelineExecutionException {
        if (value == null || value.trim().isEmpty()) {
            throw new PipelineExecutionException("Missing required field: " + field);
        }
        return value.trim();
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
                    break;
            }
        }
        return escaped.toString();
    }

    private String truncate(String response) {
        if (response.length() <= MAX_RESPONSE_CHARS) {
            return response;
        }
        return response.substring(0, MAX_RESPONSE_CHARS) + "...";
    }

    private static HttpURLConnection openHttpConnection(URL url) throws IOException {
        return (HttpURLConnection) url.openConnection();
    }

    interface ConnectionFactory {
        HttpURLConnection open(URL url) throws IOException;
    }
}
