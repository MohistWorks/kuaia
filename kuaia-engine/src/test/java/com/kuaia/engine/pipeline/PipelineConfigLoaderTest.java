package com.kuaia.engine.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineConfigLoaderTest {
    @TempDir
    Path tempDir;

    @Test
    void loadsVectorPipelineConfig() throws Exception {
        Path data = tempDir.resolve("documents.csv");
        Files.write(data, "id,content\n1,Alpha".getBytes(StandardCharsets.UTF_8));
        Path configPath = writeConfig("vector", data, "content", "embedding", "4");

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("vector", config.getName());
        assertEquals("file", config.getSource().getType());
        assertEquals("mock-vector", config.getSink().getType());
        assertEquals(2, config.getTransforms().size());
        PipelineConfig.TransformConfig embedding = config.getTransforms().get(1);
        assertEquals("mock-embedding", embedding.getType());
        assertEquals("content", embedding.getInput());
        assertEquals("embedding", embedding.getOutput());
        assertEquals(4, embedding.getDimensions());
    }

    @Test
    void loadsOpenAICompatibleEmbeddingConfig() throws Exception {
        Path configPath = tempDir.resolve("openai-compatible.yaml");
        Files.write(configPath, String.join("\n",
                "name: openai-compatible",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "transforms:",
                "  - type: embedding",
                "    provider: openai-compatible",
                "    input: content",
                "    output: embedding",
                "    model: text-embedding-3-small",
                "    apiKeyEnv: OPENAI_API_KEY",
                "    baseUrl: https://api.openai.com/v1",
                "    dimensions: 8",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig embedding = config.getTransforms().get(0);
        assertEquals("embedding", embedding.getType());
        assertEquals("openai-compatible", embedding.getProvider());
        assertEquals("content", embedding.getInput());
        assertEquals("embedding", embedding.getOutput());
        assertEquals("text-embedding-3-small", embedding.getModel());
        assertEquals("OPENAI_API_KEY", embedding.getApiKeyEnv());
        assertEquals("https://api.openai.com/v1", embedding.getBaseUrl());
        assertEquals(8, embedding.getDimensions());
    }

    @Test
    void defaultsOpenAICompatibleEmbeddingBaseUrl() throws Exception {
        Path configPath = tempDir.resolve("default-openai-compatible-base-url.yaml");
        Files.write(configPath, String.join("\n",
                "name: default-openai-compatible-base-url",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "transforms:",
                "  - type: embedding",
                "    provider: openai-compatible",
                "    input: content",
                "    output: embedding",
                "    model: text-embedding-3-small",
                "    apiKeyEnv: OPENAI_API_KEY",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("https://api.openai.com/v1", config.getTransforms().get(0).getBaseUrl());
        assertEquals(0, config.getTransforms().get(0).getDimensions());
    }

    @Test
    void rejectsMissingOpenAICompatibleEmbeddingModel() throws Exception {
        Path configPath = tempDir.resolve("missing-openai-model.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-openai-model",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "transforms:",
                "  - type: embedding",
                "    provider: openai-compatible",
                "    input: content",
                "    output: embedding",
                "    apiKeyEnv: OPENAI_API_KEY",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].model", error.getMessage());
    }

    @Test
    void rejectsMissingOpenAICompatibleEmbeddingApiKeyEnv() throws Exception {
        Path configPath = tempDir.resolve("missing-openai-api-key-env.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-openai-api-key-env",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "transforms:",
                "  - type: embedding",
                "    provider: openai-compatible",
                "    input: content",
                "    output: embedding",
                "    model: text-embedding-3-small",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].apiKeyEnv", error.getMessage());
    }

    @Test
    void rejectsUnsupportedEmbeddingProvider() throws Exception {
        Path configPath = tempDir.resolve("unsupported-embedding-provider.yaml");
        Files.write(configPath, String.join("\n",
                "name: unsupported-embedding-provider",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "transforms:",
                "  - type: embedding",
                "    provider: missing",
                "    input: content",
                "    output: embedding",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Unsupported transforms[0].provider: missing", error.getMessage());
    }

    @Test
    void loadsFileSinkConfig() throws Exception {
        Path configPath = tempDir.resolve("file-sink.yaml");
        Files.write(configPath, String.join("\n",
                "name: file-sink",
                "source:",
                "  type: file",
                "  path: data/users.csv",
                "  format: csv",
                "sink:",
                "  type: file",
                "  path: out/users.csv",
                "  format: csv",
                "  mode: overwrite").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("file", config.getSink().getType());
        assertEquals(tempDir.resolve("out/users.csv").normalize().toString(), sinkValue(config, "getPath"));
        assertEquals("csv", sinkValue(config, "getFormat"));
        assertEquals("overwrite", sinkValue(config, "getMode"));
    }

    @Test
    void loadsSkipBadRecordsErrorPolicy() throws Exception {
        Path configPath = tempDir.resolve("skip-bad-records.yaml");
        Files.write(configPath, String.join("\n",
                "name: skip-bad-records",
                "source:",
                "  type: file",
                "  path: data/users.csv",
                "  format: csv",
                "sink:",
                "  type: console",
                "errorPolicy:",
                "  mode: skip-bad-records").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("skip-bad-records", config.getErrorPolicy().getMode());
    }

    @Test
    void defaultsToFailFastErrorPolicy() throws Exception {
        Path configPath = tempDir.resolve("default-error-policy.yaml");
        Files.write(configPath, String.join("\n",
                "name: default-error-policy",
                "source:",
                "  type: file",
                "  path: data/users.csv",
                "  format: csv",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("fail-fast", config.getErrorPolicy().getMode());
    }

    @Test
    void rejectsUnsupportedErrorPolicyMode() throws Exception {
        Path configPath = tempDir.resolve("unsupported-error-policy.yaml");
        Files.write(configPath, String.join("\n",
                "name: unsupported-error-policy",
                "source:",
                "  type: file",
                "  path: data/users.csv",
                "  format: csv",
                "sink:",
                "  type: console",
                "errorPolicy:",
                "  mode: retry").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Unsupported errorPolicy.mode: retry", error.getMessage());
    }

    @Test
    void rejectsInvalidMockEmbeddingDimensions() throws Exception {
        Path data = tempDir.resolve("documents.csv");
        Files.write(data, "id,content\n1,Alpha".getBytes(StandardCharsets.UTF_8));
        Path configPath = writeConfig("invalid-dimensions", data, "content", "embedding", "zero");

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid transform.dimensions: zero", error.getMessage());
    }

    @Test
    void rejectsMissingMockEmbeddingInput() throws Exception {
        Path configPath = tempDir.resolve("missing-input.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-input",
                "source:",
                "  type: file",
                "  path: documents.csv",
                "  format: csv",
                "transforms:",
                "  - type: mock-embedding",
                "    output: embedding",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].input", error.getMessage());
    }

    @Test
    void rejectsMissingMockEmbeddingOutput() throws Exception {
        Path configPath = tempDir.resolve("missing-output.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-output",
                "source:",
                "  type: file",
                "  path: documents.csv",
                "  format: csv",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].output", error.getMessage());
    }

    private Path writeConfig(String name, Path data, String input, String output, String dimensions) throws Exception {
        Path configPath = tempDir.resolve(name + ".yaml");
        Files.write(configPath, String.join("\n",
                "name: " + name,
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: select",
                "    fields: [id, content]",
                "  - type: mock-embedding",
                "    input: " + input,
                "    output: " + output,
                "    dimensions: " + dimensions,
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));
        return configPath;
    }

    private String sinkValue(PipelineConfig config, String methodName) throws Exception {
        return (String) config.getSink().getClass().getMethod(methodName).invoke(config.getSink());
    }
}
