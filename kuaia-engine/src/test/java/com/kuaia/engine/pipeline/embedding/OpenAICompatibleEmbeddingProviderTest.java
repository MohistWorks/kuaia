package com.kuaia.engine.pipeline.embedding;

import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenAICompatibleEmbeddingProviderTest {
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void defaultsToThirtySecondTimeout() throws Exception {
        OpenAICompatibleEmbeddingProvider provider = new OpenAICompatibleEmbeddingProvider(
                "http://127.0.0.1:1/v1",
                "text-embedding-3-small",
                "OPENAI_API_KEY",
                () -> "test-key");

        assertEquals(30000, provider.getTimeoutMillis());
    }

    @Test
    void postsEmbeddingRequestAndParsesFloatVector() throws Exception {
        CapturedRequest captured = new CapturedRequest();
        server = startServer(captured, 200, "{"
                + "\"object\":\"list\","
                + "\"data\":[{\"object\":\"embedding\",\"index\":0,\"embedding\":[0.1,-2.5,3.0e-2]}],"
                + "\"model\":\"text-embedding-3-small\""
                + "}");

        OpenAICompatibleEmbeddingProvider provider = new OpenAICompatibleEmbeddingProvider(
                baseUrl(),
                "text-embedding-3-small",
                "OPENAI_API_KEY",
                () -> "test-key");

        float[] vector = provider.embed("Alpha", 3);

        assertArrayEquals(new float[]{0.1f, -2.5f, 0.03f}, vector, 0.00001f);
        assertEquals("POST", captured.method);
        assertEquals("/v1/embeddings", captured.path);
        assertEquals("Bearer test-key", captured.authorization);
        assertTrue(captured.body.contains("\"input\":\"Alpha\""), captured.body);
        assertTrue(captured.body.contains("\"model\":\"text-embedding-3-small\""), captured.body);
        assertTrue(captured.body.contains("\"encoding_format\":\"float\""), captured.body);
        assertTrue(captured.body.contains("\"dimensions\":3"), captured.body);
    }

    @Test
    void omitsDimensionsWhenNotConfigured() throws Exception {
        CapturedRequest captured = new CapturedRequest();
        server = startServer(captured, 200, "{"
                + "\"data\":[{\"embedding\":[1.0,2.0]}]"
                + "}");
        OpenAICompatibleEmbeddingProvider provider = new OpenAICompatibleEmbeddingProvider(
                baseUrl(),
                "text-embedding-3-small",
                "OPENAI_API_KEY",
                () -> "test-key");

        provider.embed("Alpha", 0);

        assertTrue(!captured.body.contains("\"dimensions\""), captured.body);
    }

    @Test
    void rejectsMissingApiKeyEnvironmentVariable() {
        OpenAICompatibleEmbeddingProvider provider = new OpenAICompatibleEmbeddingProvider(
                "http://127.0.0.1:1/v1",
                "text-embedding-3-small",
                "MISSING_OPENAI_KEY",
                () -> null);

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> provider.embed("Alpha", 0));

        assertEquals("Missing API key environment variable: MISSING_OPENAI_KEY", error.getMessage());
    }

    @Test
    void reportsNonSuccessfulHttpResponse() throws Exception {
        CapturedRequest captured = new CapturedRequest();
        server = startServer(captured, 429, "{\"error\":{\"message\":\"rate limited\"}}");
        OpenAICompatibleEmbeddingProvider provider = new OpenAICompatibleEmbeddingProvider(
                baseUrl(),
                "text-embedding-3-small",
                "OPENAI_API_KEY",
                () -> "test-key");

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> provider.embed("Alpha", 0));

        assertTrue(error.getMessage().contains("Embedding request failed with status 429"), error.getMessage());
        assertTrue(error.getMessage().contains("rate limited"), error.getMessage());
    }

    @Test
    void reportsMissingEmbeddingInResponse() throws Exception {
        CapturedRequest captured = new CapturedRequest();
        server = startServer(captured, 200, "{\"data\":[{\"object\":\"embedding\"}]}");
        OpenAICompatibleEmbeddingProvider provider = new OpenAICompatibleEmbeddingProvider(
                baseUrl(),
                "text-embedding-3-small",
                "OPENAI_API_KEY",
                () -> "test-key");

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> provider.embed("Alpha", 0));

        assertEquals("Embedding response did not contain an embedding vector", error.getMessage());
    }

    private HttpServer startServer(CapturedRequest captured, int status, String response) throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.createContext("/v1/embeddings", exchange -> {
            captured.method = exchange.getRequestMethod();
            captured.path = exchange.getRequestURI().getPath();
            captured.authorization = exchange.getRequestHeaders().getFirst("Authorization");
            captured.body = read(exchange);
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        httpServer.start();
        return httpServer;
    }

    private String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/v1";
    }

    private String read(HttpExchange exchange) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = exchange.getRequestBody().read(buffer)) >= 0) {
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
