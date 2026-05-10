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
        assertEquals(0, config.getSource().getMaxRowsPerSplit());
    }

    @Test
    void loadsFileSourceMaxRowsPerSplit() throws Exception {
        Path configPath = tempDir.resolve("file-source-split.yaml");
        Files.write(configPath, String.join("\n",
                "name: file-source-split",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "  maxRowsPerSplit: 2",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals(2, config.getSource().getMaxRowsPerSplit());
    }

    @Test
    void rejectsInvalidFileSourceMaxRowsPerSplit() throws Exception {
        Path configPath = tempDir.resolve("invalid-file-source-split.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-file-source-split",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "  maxRowsPerSplit: zero",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid source.maxRowsPerSplit: zero", error.getMessage());
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
                "    timeoutMs: 12000",
                "    batchSize: 16",
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
        assertEquals(12000, embedding.getTimeoutMs());
        assertEquals(16, embedding.getBatchSize());
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
        assertEquals(30000, config.getTransforms().get(0).getTimeoutMs());
        assertEquals(32, config.getTransforms().get(0).getBatchSize());
    }

    @Test
    void rejectsInvalidOpenAICompatibleEmbeddingTimeout() throws Exception {
        Path configPath = tempDir.resolve("invalid-openai-compatible-timeout.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-openai-compatible-timeout",
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
                "    timeoutMs: zero",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid transform.timeoutMs: zero", error.getMessage());
    }

    @Test
    void rejectsInvalidOpenAICompatibleEmbeddingBatchSize() throws Exception {
        Path configPath = tempDir.resolve("invalid-openai-compatible-batch-size.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-openai-compatible-batch-size",
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
                "    batchSize: zero",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid transform.batchSize: zero", error.getMessage());
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
    void defaultLoaderPreservesExistingPathBehavior() throws Exception {
        Path configDir = tempDir.resolve("pipelines");
        Files.createDirectories(configDir);
        Path configPath = configDir.resolve("default-paths.yaml");
        Files.write(configPath, String.join("\n",
                "name: default-paths",
                "source:",
                "  type: file",
                "  path: ../data/users.csv",
                "  format: csv",
                "sink:",
                "  type: file",
                "  path: ../out/users.csv",
                "  format: csv",
                "checkpoint:",
                "  stateDir: .kuaia/state/default-paths").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals(tempDir.resolve("data/users.csv").normalize().toString(), config.getSource().getPath());
        assertEquals(tempDir.resolve("out/users.csv").normalize().toString(), config.getSink().getPath());
        assertEquals(".kuaia/state/default-paths", config.getCheckpoint().getStateDir());
    }

    @Test
    void restrictedLoaderRejectsSourcePathOutsideYamlDirectoryAndRepoKuaia() throws Exception {
        Path repoRoot = fakeRepoRoot();
        Path configDir = repoRoot.resolve("examples");
        Files.createDirectories(configDir);
        Path configPath = configDir.resolve("restricted-source.yaml");
        Files.write(configPath, String.join("\n",
                "name: restricted-source",
                "source:",
                "  type: file",
                "  path: ../private/users.csv",
                "  format: csv",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader(true).load(configPath));

        assertEquals("Local path escapes allowed directories: source.path", error.getMessage());
    }

    @Test
    void restrictedLoaderAllowsYamlDirectoryAndRepoKuaiaPaths() throws Exception {
        Path repoRoot = fakeRepoRoot();
        Path configDir = repoRoot.resolve("examples");
        Files.createDirectories(configDir.resolve("data"));
        Path configPath = configDir.resolve("restricted-allowed.yaml");
        Files.write(configPath, String.join("\n",
                "name: restricted-allowed",
                "source:",
                "  type: file",
                "  path: data/users.csv",
                "  format: csv",
                "sink:",
                "  type: file",
                "  path: ../.kuaia/output/users.csv",
                "  format: csv",
                "checkpoint:",
                "  stateDir: .kuaia/state/restricted-allowed").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader(true).load(configPath);

        assertEquals(configDir.resolve("data/users.csv").normalize().toString(), config.getSource().getPath());
        assertEquals(repoRoot.resolve(".kuaia/output/users.csv").normalize().toString(), config.getSink().getPath());
        assertEquals(
                repoRoot.resolve(".kuaia/state/restricted-allowed").normalize().toString(),
                config.getCheckpoint().getStateDir());
    }

    @Test
    void restrictedLoaderRejectsCheckpointPathOutsideAllowedDirectories() throws Exception {
        Path repoRoot = fakeRepoRoot();
        Path configDir = repoRoot.resolve("examples");
        Files.createDirectories(configDir.resolve("data"));
        Path configPath = configDir.resolve("restricted-checkpoint.yaml");
        Files.write(configPath, String.join("\n",
                "name: restricted-checkpoint",
                "source:",
                "  type: file",
                "  path: data/users.csv",
                "  format: csv",
                "sink:",
                "  type: console",
                "checkpoint:",
                "  stateDir: ../runtime-state").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader(true).load(configPath));

        assertEquals("Local path escapes allowed directories: checkpoint.stateDir", error.getMessage());
    }

    @Test
    void loadsQdrantSinkConfig() throws Exception {
        Path configPath = tempDir.resolve("qdrant-sink.yaml");
        Files.write(configPath, String.join("\n",
                "name: qdrant-sink",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "    dimensions: 4",
                "sink:",
                "  type: qdrant",
                "  url: http://localhost:6333",
                "  collection: kuaia_docs",
                "  apiKeyEnv: QDRANT_API_KEY",
                "  idField: id",
                "  vectorField: embedding").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("qdrant", config.getSink().getType());
        assertEquals("http://localhost:6333", config.getSink().getUrl());
        assertEquals("kuaia_docs", config.getSink().getCollection());
        assertEquals("QDRANT_API_KEY", config.getSink().getApiKeyEnv());
        assertEquals("id", config.getSink().getIdField());
        assertEquals("embedding", config.getSink().getVectorField());
        assertEquals(true, config.getSink().isWait());
    }

    @Test
    void loadsQdrantSinkWaitFlag() throws Exception {
        Path configPath = tempDir.resolve("qdrant-sink-nowait.yaml");
        Files.write(configPath, String.join("\n",
                "name: qdrant-sink-nowait",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "sink:",
                "  type: qdrant",
                "  url: http://localhost:6333",
                "  collection: kuaia_docs",
                "  idField: id",
                "  vectorField: embedding",
                "  wait: false").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals(false, config.getSink().isWait());
    }

    @Test
    void rejectsMissingQdrantCollection() throws Exception {
        Path configPath = tempDir.resolve("missing-qdrant-collection.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-qdrant-collection",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "sink:",
                "  type: qdrant",
                "  url: http://localhost:6333",
                "  idField: id",
                "  vectorField: embedding").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: sink.collection", error.getMessage());
    }

    @Test
    void loadsPostgresSourceConfig() throws Exception {
        Path configPath = tempDir.resolve("postgres-source.yaml");
        Files.write(configPath, String.join("\n",
                "name: postgres-source",
                "source:",
                "  type: postgres",
                "  url: jdbc:postgresql://localhost:5432/kuaia",
                "  userEnv: KUAIA_POSTGRES_USER",
                "  passwordEnv: KUAIA_POSTGRES_PASSWORD",
                "  query: select id, content from documents order by id",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("postgres", config.getSource().getType());
        assertEquals("jdbc:postgresql://localhost:5432/kuaia", config.getSource().getUrl());
        assertEquals("KUAIA_POSTGRES_USER", config.getSource().getUserEnv());
        assertEquals("KUAIA_POSTGRES_PASSWORD", config.getSource().getPasswordEnv());
        assertEquals("select id, content from documents order by id", config.getSource().getQuery());
    }

    @Test
    void rejectsMissingPostgresQuery() throws Exception {
        Path configPath = tempDir.resolve("missing-postgres-query.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-postgres-query",
                "source:",
                "  type: postgres",
                "  url: jdbc:postgresql://localhost:5432/kuaia",
                "  userEnv: KUAIA_POSTGRES_USER",
                "  passwordEnv: KUAIA_POSTGRES_PASSWORD",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: source.query", error.getMessage());
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

    private Path fakeRepoRoot() throws Exception {
        Path repoRoot = tempDir.resolve("repo");
        Files.createDirectories(repoRoot.resolve("kuaia-engine"));
        Files.write(repoRoot.resolve("pom.xml"), "<project/>".getBytes(StandardCharsets.UTF_8));
        return repoRoot;
    }
}
