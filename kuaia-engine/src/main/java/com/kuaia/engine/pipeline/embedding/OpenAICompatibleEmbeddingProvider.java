package com.kuaia.engine.pipeline.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class OpenAICompatibleEmbeddingProvider implements EmbeddingProvider {
    public static final String DEFAULT_BASE_URL = "https://api.openai.com/v1";
    public static final int DEFAULT_TIMEOUT_MILLIS = 30_000;
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final String baseUrl;
    private final String model;
    private final String apiKeyEnv;
    private final int timeoutMillis;
    private final Supplier<String> apiKeySupplier;

    public OpenAICompatibleEmbeddingProvider(String baseUrl, String model, String apiKeyEnv) {
        this(baseUrl, model, apiKeyEnv, DEFAULT_TIMEOUT_MILLIS);
    }

    public OpenAICompatibleEmbeddingProvider(String baseUrl, String model, String apiKeyEnv, int timeoutMillis) {
        this(baseUrl, model, apiKeyEnv, timeoutMillis, () -> System.getenv(apiKeyEnv));
    }

    OpenAICompatibleEmbeddingProvider(
            String baseUrl,
            String model,
            String apiKeyEnv,
            Supplier<String> apiKeySupplier) {
        this(baseUrl, model, apiKeyEnv, DEFAULT_TIMEOUT_MILLIS, apiKeySupplier);
    }

    OpenAICompatibleEmbeddingProvider(
            String baseUrl,
            String model,
            String apiKeyEnv,
            int timeoutMillis,
            Supplier<String> apiKeySupplier) {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be positive");
        }
        this.baseUrl = trimTrailingSlash(baseUrl);
        this.model = model;
        this.apiKeyEnv = apiKeyEnv;
        this.timeoutMillis = timeoutMillis;
        this.apiKeySupplier = apiKeySupplier;
    }

    @Override
    public float[] embed(String input, int dimensions) throws PipelineExecutionException {
        return requestEmbedding(requestBody(input, dimensions), 1).get(0);
    }

    @Override
    public List<float[]> embedBatch(List<String> inputs, int dimensions) throws PipelineExecutionException {
        if (inputs.isEmpty()) {
            return new ArrayList<>();
        }
        return requestEmbedding(batchRequestBody(inputs, dimensions), inputs.size());
    }

    private List<float[]> requestEmbedding(String requestBody, int expectedCount) throws PipelineExecutionException {
        String apiKey = apiKeySupplier.get();
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new PipelineExecutionException("Missing API key environment variable: " + apiKeyEnv);
        }

        HttpURLConnection connection = null;
        try {
            URL endpoint = new URL(baseUrl + "/embeddings");
            connection = (HttpURLConnection) endpoint.openConnection();
            connection.setConnectTimeout(timeoutMillis);
            connection.setReadTimeout(timeoutMillis);
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Bearer " + apiKey);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");

            byte[] body = requestBody.getBytes(StandardCharsets.UTF_8);
            connection.setFixedLengthStreamingMode(body.length);
            try (OutputStream output = connection.getOutputStream()) {
                output.write(body);
            }

            int status = connection.getResponseCode();
            String response = read(status >= 200 && status < 300
                    ? connection.getInputStream()
                    : connection.getErrorStream());
            if (status < 200 || status >= 300) {
                throw new PipelineExecutionException(
                        "Embedding request failed with status " + status + ": " + truncate(response));
            }
            return parseEmbeddings(response, expectedCount);
        } catch (PipelineExecutionException e) {
            throw e;
        } catch (IOException e) {
            throw new PipelineExecutionException("Embedding request failed: " + e.getMessage(), e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private String requestBody(String input, int dimensions) {
        StringBuilder body = new StringBuilder();
        body.append("{")
                .append("\"input\":\"").append(escapeJson(input)).append("\",")
                .append("\"model\":\"").append(escapeJson(model)).append("\",")
                .append("\"encoding_format\":\"float\"");
        if (dimensions > 0) {
            body.append(",\"dimensions\":").append(dimensions);
        }
        body.append("}");
        return body.toString();
    }

    private String batchRequestBody(List<String> inputs, int dimensions) {
        StringBuilder body = new StringBuilder();
        body.append("{")
                .append("\"input\":[");
        for (int i = 0; i < inputs.size(); i++) {
            if (i > 0) {
                body.append(",");
            }
            body.append("\"").append(escapeJson(inputs.get(i))).append("\"");
        }
        body.append("],")
                .append("\"model\":\"").append(escapeJson(model)).append("\",")
                .append("\"encoding_format\":\"float\"");
        if (dimensions > 0) {
            body.append(",\"dimensions\":").append(dimensions);
        }
        body.append("}");
        return body.toString();
    }

    private List<float[]> parseEmbeddings(String response, int expectedCount) throws PipelineExecutionException {
        JsonNode data = parseDataArray(response);
        if (data.size() != expectedCount) {
            throw new PipelineExecutionException(
                    "Embedding response returned " + data.size() + " embeddings but expected " + expectedCount);
        }
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < expectedCount; i++) {
            vectors.add(null);
        }

        for (int fallbackIndex = 0; fallbackIndex < data.size(); fallbackIndex++) {
            JsonNode item = data.get(fallbackIndex);
            int index = parseIndex(item, fallbackIndex);
            if (index < 0 || index >= expectedCount) {
                throw new PipelineExecutionException("Embedding response index out of range: " + index);
            }
            if (vectors.get(index) != null) {
                throw new PipelineExecutionException("Embedding response contained duplicate embedding index: " + index);
            }
            vectors.set(index, parseVector(item.get("embedding")));
        }

        for (float[] vector : vectors) {
            if (vector == null) {
                throw new PipelineExecutionException("Embedding response did not contain an embedding vector");
            }
        }
        return vectors;
    }

    private JsonNode parseDataArray(String response) throws PipelineExecutionException {
        try {
            JsonNode root = JSON_MAPPER.readTree(response);
            JsonNode data = root == null ? null : root.get("data");
            if (data == null || !data.isArray()) {
                throw new PipelineExecutionException("Embedding response did not contain an embedding vector");
            }
            return data;
        } catch (PipelineExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new PipelineExecutionException("Embedding response did not contain an embedding vector");
        }
    }

    private int parseIndex(JsonNode item, int fallbackIndex) throws PipelineExecutionException {
        JsonNode index = item == null ? null : item.get("index");
        if (index == null) {
            return fallbackIndex;
        }
        if (!index.isIntegralNumber() || !index.canConvertToInt()) {
            throw new PipelineExecutionException("Embedding response did not contain an embedding vector");
        }
        return index.asInt();
    }

    private float[] parseVector(JsonNode embedding) throws PipelineExecutionException {
        if (embedding == null || !embedding.isArray() || embedding.size() == 0) {
            throw new PipelineExecutionException("Embedding response did not contain an embedding vector");
        }
        float[] vector = new float[embedding.size()];
        for (int i = 0; i < embedding.size(); i++) {
            JsonNode value = embedding.get(i);
            if (!value.isNumber()) {
                throw new PipelineExecutionException("Invalid embedding value: " + value.asText());
            }
            vector[i] = (float) value.asDouble();
        }
        return vector;
    }

    private String read(InputStream input) throws IOException {
        if (input == null) {
            return "";
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            bytes.write(buffer, 0, read);
        }
        return bytes.toString(StandardCharsets.UTF_8.name());
    }

    private String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
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
                    if (c < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
                    break;
            }
        }
        return escaped.toString();
    }

    private static String trimTrailingSlash(String value) {
        String trimmed = value.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private String truncate(String value) {
        if (value.length() <= 1000) {
            return value;
        }
        return value.substring(0, 1000);
    }

    int getTimeoutMillis() {
        return timeoutMillis;
    }
}
