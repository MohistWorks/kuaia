package com.kuaia.engine.pipeline.embedding;

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

    private static final int TIMEOUT_MILLIS = 30_000;

    private final String baseUrl;
    private final String model;
    private final String apiKeyEnv;
    private final Supplier<String> apiKeySupplier;

    public OpenAICompatibleEmbeddingProvider(String baseUrl, String model, String apiKeyEnv) {
        this(baseUrl, model, apiKeyEnv, () -> System.getenv(apiKeyEnv));
    }

    OpenAICompatibleEmbeddingProvider(
            String baseUrl,
            String model,
            String apiKeyEnv,
            Supplier<String> apiKeySupplier) {
        this.baseUrl = trimTrailingSlash(baseUrl);
        this.model = model;
        this.apiKeyEnv = apiKeyEnv;
        this.apiKeySupplier = apiKeySupplier;
    }

    @Override
    public float[] embed(String input, int dimensions) throws PipelineExecutionException {
        String apiKey = apiKeySupplier.get();
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new PipelineExecutionException("Missing API key environment variable: " + apiKeyEnv);
        }

        HttpURLConnection connection = null;
        try {
            URL endpoint = new URL(baseUrl + "/embeddings");
            connection = (HttpURLConnection) endpoint.openConnection();
            connection.setConnectTimeout(TIMEOUT_MILLIS);
            connection.setReadTimeout(TIMEOUT_MILLIS);
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Bearer " + apiKey);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");

            byte[] body = requestBody(input, dimensions).getBytes(StandardCharsets.UTF_8);
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
            return parseEmbedding(response);
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

    private float[] parseEmbedding(String response) throws PipelineExecutionException {
        int key = response.indexOf("\"embedding\"");
        if (key < 0) {
            throw new PipelineExecutionException("Embedding response did not contain an embedding vector");
        }
        int arrayStart = response.indexOf('[', key);
        if (arrayStart < 0) {
            throw new PipelineExecutionException("Embedding response did not contain an embedding vector");
        }
        int arrayEnd = findArrayEnd(response, arrayStart);
        if (arrayEnd < 0) {
            throw new PipelineExecutionException("Embedding response did not contain an embedding vector");
        }

        String body = response.substring(arrayStart + 1, arrayEnd).trim();
        if (body.isEmpty()) {
            throw new PipelineExecutionException("Embedding response did not contain an embedding vector");
        }

        String[] parts = body.split(",");
        List<Float> values = new ArrayList<>();
        for (String part : parts) {
            String value = part.trim();
            if (value.isEmpty()) {
                throw new PipelineExecutionException("Invalid embedding value: " + part);
            }
            try {
                values.add(Float.parseFloat(value));
            } catch (NumberFormatException e) {
                throw new PipelineExecutionException("Invalid embedding value: " + value, e);
            }
        }

        float[] vector = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            vector[i] = values.get(i);
        }
        return vector;
    }

    private int findArrayEnd(String text, int arrayStart) {
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        for (int i = arrayStart; i < text.length(); i++) {
            char c = text.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                continue;
            }
            if (inString) {
                continue;
            }
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
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
}
