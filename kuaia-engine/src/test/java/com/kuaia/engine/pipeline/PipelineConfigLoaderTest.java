package com.kuaia.engine.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineConfigLoaderTest {
    @TempDir
    Path tempDir;

    @Test
    void loadFromStringMatchesLoadPath(@TempDir Path tmp) throws Exception {
        String yaml = String.join("\n",
                "name: string-parse-demo",
                "source:",
                "  type: file",
                "  path: " + tmp.resolve("in.csv"),
                "  format: csv",
                "sink:",
                "  type: file",
                "  path: " + tmp.resolve("out.csv"),
                "  format: csv",
                "  mode: overwrite");
        Path file = tmp.resolve("pipeline.yaml");
        Files.write(file, yaml.getBytes(StandardCharsets.UTF_8));

        PipelineConfig fromPath = new PipelineConfigLoader().load(file);
        PipelineConfig fromString = new PipelineConfigLoader().loadFromString(yaml);

        assertEquals(fromPath.getName(), fromString.getName());
        assertEquals(fromPath.getSource().getType(), fromString.getSource().getType());
        assertEquals(fromPath.getSink().getType(), fromString.getSink().getType());
    }

    @Test
    void loadFromStringResolvesRelativePathsAgainstCwd() throws Exception {
        // Relative source/sink paths must resolve against the coordinator process CWD via the
        // synthetic config path — this exercises the resolveLocalPath branch that dereferences
        // configPath (a null synthetic path would NPE here).
        String yaml = String.join("\n",
                "name: rel-path-demo",
                "source:",
                "  type: file",
                "  path: sub/in.csv",
                "  format: csv",
                "sink:",
                "  type: file",
                "  path: sub/out.csv",
                "  format: csv",
                "  mode: overwrite");

        PipelineConfig cfg = new PipelineConfigLoader(false).loadFromString(yaml);

        Path cwd = Paths.get("").toAbsolutePath();
        assertEquals(
                cwd.resolve("sub/in.csv").normalize(),
                Paths.get(cfg.getSource().getPath()).normalize(),
                "relative source path should resolve against the process CWD");
        assertEquals(
                cwd.resolve("sub/out.csv").normalize(),
                Paths.get(cfg.getSink().getPath()).normalize(),
                "relative sink path should resolve against the process CWD");
    }

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
    void loadsJsonlFileSourceFormat() throws Exception {
        Path configPath = tempDir.resolve("jsonl-source.yaml");
        Files.write(configPath, String.join("\n",
                "name: jsonl-source",
                "source:",
                "  type: file",
                "  path: data/documents.jsonl",
                "  format: jsonl",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("jsonl", config.getSource().getFormat());
    }

    @Test
    void loadsS3SourceConfig() throws Exception {
        Path configPath = tempDir.resolve("s3-source.yaml");
        Files.write(configPath, String.join("\n",
                "name: s3-source",
                "source:",
                "  type: s3",
                "  endpoint: http://127.0.0.1:9000",
                "  region: us-east-1",
                "  bucket: kuaia-docs",
                "  prefix: docs/",
                "  accessKeyEnv: KUAIA_S3_ACCESS_KEY",
                "  secretKeyEnv: KUAIA_S3_SECRET_KEY",
                "  pathStyleAccess: true",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("s3", config.getSource().getType());
        assertEquals("http://127.0.0.1:9000", config.getSource().getEndpoint());
        assertEquals("us-east-1", config.getSource().getRegion());
        assertEquals("kuaia-docs", config.getSource().getBucket());
        assertEquals("docs/", config.getSource().getPrefix());
        assertEquals("KUAIA_S3_ACCESS_KEY", config.getSource().getAccessKeyEnv());
        assertEquals("KUAIA_S3_SECRET_KEY", config.getSource().getSecretKeyEnv());
        assertEquals(true, config.getSource().isPathStyleAccess());
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
    void rejectsFileSourceSplitConfigForPostgresSource() throws Exception {
        Path configPath = tempDir.resolve("postgres-source-split.yaml");
        Files.write(configPath, String.join("\n",
                "name: postgres-source-split",
                "source:",
                "  type: postgres",
                "  url: jdbc:postgresql://localhost:5432/kuaia",
                "  userEnv: PGUSER",
                "  passwordEnv: PGPASSWORD",
                "  query: SELECT id, content FROM documents",
                "  maxRowsPerSplit: 2",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("source.maxRowsPerSplit is only supported for file formats csv and jsonl", error.getMessage());
    }

    @Test
    void loadsPostgresSourceFetchSize() throws Exception {
        Path configPath = tempDir.resolve("postgres-fetch-size.yaml");
        Files.write(configPath, String.join("\n",
                "name: postgres-fetch-size",
                "source:",
                "  type: postgres",
                "  url: jdbc:postgresql://localhost:5432/kuaia",
                "  userEnv: PGUSER",
                "  passwordEnv: PGPASSWORD",
                "  query: SELECT id, content FROM documents",
                "  fetchSize: 128",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals(128, config.getSource().getFetchSize());
    }

    @Test
    void rejectsInvalidPostgresSourceFetchSize() throws Exception {
        Path configPath = tempDir.resolve("invalid-postgres-fetch-size.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-postgres-fetch-size",
                "source:",
                "  type: postgres",
                "  url: jdbc:postgresql://localhost:5432/kuaia",
                "  userEnv: PGUSER",
                "  passwordEnv: PGPASSWORD",
                "  query: SELECT id, content FROM documents",
                "  fetchSize: zero",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid source.fetchSize: zero", error.getMessage());
    }

    @Test
    void rejectsPostgresFetchSizeForFileSource() throws Exception {
        Path configPath = tempDir.resolve("file-source-fetch-size.yaml");
        Files.write(configPath, String.join("\n",
                "name: file-source-fetch-size",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "  fetchSize: 128",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("source.fetchSize is only supported for JDBC source types", error.getMessage());
    }

    @Test
    void rejectsJdbcOnlyFieldsForFileSource() throws Exception {
        assertFileSourceFieldRejected("url", "jdbc:mysql://localhost:3306/kuaia",
                "source.url is only supported for JDBC source types");
        assertFileSourceFieldRejected("userEnv", "KUAIA_USER",
                "source.userEnv is only supported for JDBC source types");
        assertFileSourceFieldRejected("passwordEnv", "KUAIA_PASSWORD",
                "source.passwordEnv is only supported for JDBC source types");
        assertFileSourceFieldRejected("query", "select id from documents",
                "source.query is only supported for JDBC source types");
    }

    @Test
    void rejectsFileOnlyFieldsForJdbcSource() throws Exception {
        assertJdbcSourceFieldRejected("path", "data/documents.csv",
                "source.path is only supported for local source types");
        assertJdbcSourceFieldRejected("format", "csv",
                "source.format is only supported for source.type: file");
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
    void loadsChunkTransformConfig() throws Exception {
        Path configPath = tempDir.resolve("chunk-transform.yaml");
        Files.write(configPath, String.join("\n",
                "name: chunk-transform",
                "source:",
                "  type: file",
                "  path: data/documents.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: chunk",
                "    input: content",
                "    output: chunk",
                "    chunkSize: 5",
                "    overlap: 1",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig chunk = config.getTransforms().get(0);
        assertEquals("chunk", chunk.getType());
        assertEquals("content", chunk.getInput());
        assertEquals("chunk", chunk.getOutput());
        assertEquals(5, chunk.getChunkSize());
        assertEquals(1, chunk.getOverlap());
        assertEquals(false, chunk.isDropInput());
        assertEquals(false, chunk.isIncludeOffsets());
    }

    @Test
    void loadsChunkTransformPayloadControls() throws Exception {
        Path configPath = tempDir.resolve("chunk-transform-payload-controls.yaml");
        Files.write(configPath, String.join("\n",
                "name: chunk-transform-payload-controls",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: chunk",
                "    input: content",
                "    output: chunk",
                "    chunkSize: 500",
                "    dropInput: true",
                "    includeOffsets: true",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig chunk = config.getTransforms().get(0);
        assertEquals(true, chunk.isDropInput());
        assertEquals(true, chunk.isIncludeOffsets());
    }

    @Test
    void defaultsChunkTransformOverlapToZero() throws Exception {
        Path configPath = tempDir.resolve("chunk-transform-default-overlap.yaml");
        Files.write(configPath, String.join("\n",
                "name: chunk-transform-default-overlap",
                "source:",
                "  type: file",
                "  path: data/documents.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: chunk",
                "    input: content",
                "    output: chunk",
                "    chunkSize: 5",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals(0, config.getTransforms().get(0).getOverlap());
    }

    @Test
    void rejectsInvalidChunkTransformPayloadBoolean() throws Exception {
        Path configPath = tempDir.resolve("invalid-chunk-transform-payload-boolean.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-chunk-transform-payload-boolean",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: chunk",
                "    input: content",
                "    output: chunk",
                "    chunkSize: 500",
                "    includeOffsets: yes",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid transform.includeOffsets: yes", error.getMessage());
    }

    @Test
    void loadsTrimTransformConfig() throws Exception {
        Path configPath = tempDir.resolve("trim-transform.yaml");
        Files.write(configPath, String.join("\n",
                "name: trim-transform",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: trim",
                "    field: content",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig trim = config.getTransforms().get(0);
        assertEquals("trim", trim.getType());
        assertEquals("content", trim.getInput());
    }

    @Test
    void rejectsMissingTrimTransformField() throws Exception {
        Path configPath = tempDir.resolve("missing-trim-field.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-trim-field",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: trim",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].field", error.getMessage());
    }

    @Test
    void loadsFilterTransformConfig() throws Exception {
        Path configPath = tempDir.resolve("filter-transform.yaml");
        Files.write(configPath, String.join("\n",
                "name: filter-transform",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: not-empty",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig filter = config.getTransforms().get(0);
        assertEquals("filter", filter.getType());
        assertEquals("content", filter.getInput());
        assertEquals("not-empty", filter.getOp());
    }

    @Test
    void loadsMinLengthFilterTransformConfig() throws Exception {
        Path configPath = tempDir.resolve("min-length-filter-transform.yaml");
        Files.write(configPath, String.join("\n",
                "name: min-length-filter-transform",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: min-length",
                "    minLength: 12",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig filter = config.getTransforms().get(0);
        assertEquals("filter", filter.getType());
        assertEquals("content", filter.getInput());
        assertEquals("min-length", filter.getOp());
        assertEquals(12, filter.getMinLength());
    }

    @Test
    void loadsContainsFilterTransformConfig() throws Exception {
        Path configPath = tempDir.resolve("contains-filter-transform.yaml");
        Files.write(configPath, String.join("\n",
                "name: contains-filter-transform",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: contains",
                "    value: Alpha",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig filter = config.getTransforms().get(0);
        assertEquals("filter", filter.getType());
        assertEquals("content", filter.getInput());
        assertEquals("contains", filter.getOp());
        assertEquals("Alpha", filter.getValue());
    }

    @Test
    void loadsPrefixAndSuffixFilterTransformConfig() throws Exception {
        Path configPath = tempDir.resolve("prefix-suffix-filter-transform.yaml");
        Files.write(configPath, String.join("\n",
                "name: prefix-suffix-filter-transform",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: starts-with",
                "    value: Alpha",
                "  - type: filter",
                "    field: content",
                "    op: ends-with",
                "    value: done",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig prefix = config.getTransforms().get(0);
        assertEquals("filter", prefix.getType());
        assertEquals("content", prefix.getInput());
        assertEquals("starts-with", prefix.getOp());
        assertEquals("Alpha", prefix.getValue());

        PipelineConfig.TransformConfig suffix = config.getTransforms().get(1);
        assertEquals("filter", suffix.getType());
        assertEquals("content", suffix.getInput());
        assertEquals("ends-with", suffix.getOp());
        assertEquals("done", suffix.getValue());
    }

    @Test
    void loadsEqualityFilterTransformConfig() throws Exception {
        Path configPath = tempDir.resolve("equality-filter-transform.yaml");
        Files.write(configPath, String.join("\n",
                "name: equality-filter-transform",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: equals",
                "    value: alfa",
                "  - type: filter",
                "    field: content",
                "    op: not-equals",
                "    value: beta",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig equals = config.getTransforms().get(0);
        assertEquals("filter", equals.getType());
        assertEquals("content", equals.getInput());
        assertEquals("equals", equals.getOp());
        assertEquals("alfa", equals.getValue());

        PipelineConfig.TransformConfig notEquals = config.getTransforms().get(1);
        assertEquals("filter", notEquals.getType());
        assertEquals("content", notEquals.getInput());
        assertEquals("not-equals", notEquals.getOp());
        assertEquals("beta", notEquals.getValue());
    }

    @Test
    void loadsLongComparisonFilterTransformConfig() throws Exception {
        Path configPath = tempDir.resolve("long-comparison-filter-transform.yaml");
        Files.write(configPath, String.join("\n",
                "name: long-comparison-filter-transform",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: id",
                "    op: greater-than",
                "    value: 10",
                "  - type: filter",
                "    field: id",
                "    op: greater-than-or-equal",
                "    value: 11",
                "  - type: filter",
                "    field: id",
                "    op: less-than",
                "    value: 20",
                "  - type: filter",
                "    field: id",
                "    op: less-than-or-equal",
                "    value: 21",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("greater-than", config.getTransforms().get(0).getOp());
        assertEquals("10", config.getTransforms().get(0).getValue());
        assertEquals("greater-than-or-equal", config.getTransforms().get(1).getOp());
        assertEquals("11", config.getTransforms().get(1).getValue());
        assertEquals("less-than", config.getTransforms().get(2).getOp());
        assertEquals("20", config.getTransforms().get(2).getValue());
        assertEquals("less-than-or-equal", config.getTransforms().get(3).getOp());
        assertEquals("21", config.getTransforms().get(3).getValue());
    }

    @Test
    void loadsLowercaseTransformConfig() throws Exception {
        Path configPath = tempDir.resolve("lowercase-transform.yaml");
        Files.write(configPath, String.join("\n",
                "name: lowercase-transform",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: lowercase",
                "    field: content",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig lowercase = config.getTransforms().get(0);
        assertEquals("lowercase", lowercase.getType());
        assertEquals("content", lowercase.getInput());
    }

    @Test
    void loadsReplaceTransformConfig() throws Exception {
        Path configPath = tempDir.resolve("replace-transform.yaml");
        Files.write(configPath, String.join("\n",
                "name: replace-transform",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: replace",
                "    field: content",
                "    target: ph",
                "    replacement: f",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig replace = config.getTransforms().get(0);
        assertEquals("replace", replace.getType());
        assertEquals("content", replace.getInput());
        assertEquals("ph", replace.getFrom());
        assertEquals("f", replace.getTo());
    }

    @Test
    void loadsReplaceTransformWithDefaultEmptyReplacement() throws Exception {
        Path configPath = tempDir.resolve("replace-transform-default-empty.yaml");
        Files.write(configPath, String.join("\n",
                "name: replace-transform-default-empty",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: replace",
                "    field: content",
                "    target: beta",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        PipelineConfig.TransformConfig replace = config.getTransforms().get(0);
        assertEquals("replace", replace.getType());
        assertEquals("content", replace.getInput());
        assertEquals("beta", replace.getFrom());
        assertEquals("", replace.getTo());
    }

    @Test
    void rejectsMissingReplaceTransformTarget() throws Exception {
        Path configPath = tempDir.resolve("missing-replace-target.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-replace-target",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: replace",
                "    field: content",
                "    replacement: f",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].target", error.getMessage());
    }

    @Test
    void rejectsMissingFilterTransformOp() throws Exception {
        Path configPath = tempDir.resolve("missing-filter-op.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-filter-op",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].op", error.getMessage());
    }

    @Test
    void rejectsUnsupportedFilterTransformOp() throws Exception {
        Path configPath = tempDir.resolve("unsupported-filter-op.yaml");
        Files.write(configPath, String.join("\n",
                "name: unsupported-filter-op",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: regex",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Unsupported transforms[0].op: regex", error.getMessage());
    }

    @Test
    void rejectsMissingMinLengthFilterTransformMinLength() throws Exception {
        Path configPath = tempDir.resolve("missing-min-length-filter-min-length.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-min-length-filter-min-length",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: min-length",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].minLength", error.getMessage());
    }

    @Test
    void rejectsInvalidMinLengthFilterTransformMinLength() throws Exception {
        Path configPath = tempDir.resolve("invalid-min-length-filter-min-length.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-min-length-filter-min-length",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: min-length",
                "    minLength: 0",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid transform.minLength: 0", error.getMessage());
    }

    @Test
    void rejectsMissingContainsFilterTransformValue() throws Exception {
        Path configPath = tempDir.resolve("missing-contains-filter-value.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-contains-filter-value",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: contains",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].value", error.getMessage());
    }

    @Test
    void rejectsMissingStartsWithFilterTransformValue() throws Exception {
        Path configPath = tempDir.resolve("missing-starts-with-filter-value.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-starts-with-filter-value",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: starts-with",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].value", error.getMessage());
    }

    @Test
    void rejectsMissingEqualsFilterTransformValue() throws Exception {
        Path configPath = tempDir.resolve("missing-equals-filter-value.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-equals-filter-value",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: content",
                "    op: equals",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].value", error.getMessage());
    }

    @Test
    void rejectsInvalidLongComparisonFilterTransformValue() throws Exception {
        Path configPath = tempDir.resolve("invalid-long-comparison-filter-value.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-long-comparison-filter-value",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: filter",
                "    field: id",
                "    op: greater-than",
                "    value: abc",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid transform.value: abc", error.getMessage());
    }

    @Test
    void rejectsMissingChunkTransformChunkSize() throws Exception {
        Path configPath = tempDir.resolve("missing-chunk-size.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-chunk-size",
                "source:",
                "  type: file",
                "  path: data/documents.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: chunk",
                "    input: content",
                "    output: chunk",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: transforms[0].chunkSize", error.getMessage());
    }

    @Test
    void rejectsInvalidChunkTransformChunkSize() throws Exception {
        Path configPath = tempDir.resolve("invalid-chunk-size.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-chunk-size",
                "source:",
                "  type: file",
                "  path: data/documents.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: chunk",
                "    input: content",
                "    output: chunk",
                "    chunkSize: zero",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid transform.chunkSize: zero", error.getMessage());
    }

    @Test
    void rejectsChunkTransformOverlapAtOrAboveChunkSize() throws Exception {
        Path configPath = tempDir.resolve("invalid-chunk-overlap.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-chunk-overlap",
                "source:",
                "  type: file",
                "  path: data/documents.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: chunk",
                "    input: content",
                "    output: chunk",
                "    chunkSize: 5",
                "    overlap: 5",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("transform.overlap must be smaller than transform.chunkSize", error.getMessage());
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
    void loadsJsonlFileSinkConfig() throws Exception {
        Path configPath = tempDir.resolve("jsonl-file-sink.yaml");
        Files.write(configPath, String.join("\n",
                "name: jsonl-file-sink",
                "source:",
                "  type: file",
                "  path: data/documents.jsonl",
                "  format: jsonl",
                "sink:",
                "  type: file",
                "  path: out/documents.jsonl",
                "  format: jsonl",
                "  mode: overwrite").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("file", config.getSink().getType());
        assertEquals(tempDir.resolve("out/documents.jsonl").normalize().toString(), sinkValue(config, "getPath"));
        assertEquals("jsonl", sinkValue(config, "getFormat"));
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
    void loadsQdrantSinkTimeout() throws Exception {
        Path configPath = tempDir.resolve("qdrant-sink-timeout.yaml");
        Files.write(configPath, String.join("\n",
                "name: qdrant-sink-timeout",
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
                "  timeoutMs: 12000").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals(12000, config.getSink().getTimeoutMs());
    }

    @Test
    void loadsQdrantPayloadFields() throws Exception {
        Path configPath = tempDir.resolve("qdrant-sink-payload-fields.yaml");
        Files.write(configPath, String.join("\n",
                "name: qdrant-sink-payload-fields",
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
                "  payloadFields: [id, source]").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals(java.util.Arrays.asList("id", "source"), config.getSink().getPayloadFields());
    }

    @Test
    void loadsQdrantChunkPointIdDefaults() throws Exception {
        Path configPath = tempDir.resolve("qdrant-sink-chunk-id-defaults.yaml");
        Files.write(configPath, String.join("\n",
                "name: qdrant-sink-chunk-id-defaults",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "transforms:",
                "  - type: chunk",
                "    input: content",
                "    output: chunk",
                "    chunkSize: 500",
                "sink:",
                "  type: qdrant",
                "  url: http://localhost:6333",
                "  collection: kuaia_chunks",
                "  idField: id",
                "  vectorField: embedding",
                "  chunkIndexField: chunk_index").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("chunk_index", config.getSink().getChunkIndexField());
        assertEquals(1_000_000L, config.getSink().getChunkIdMultiplier());
    }

    @Test
    void loadsQdrantChunkPointIdMultiplier() throws Exception {
        Path configPath = tempDir.resolve("qdrant-sink-chunk-id-multiplier.yaml");
        Files.write(configPath, String.join("\n",
                "name: qdrant-sink-chunk-id-multiplier",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "sink:",
                "  type: qdrant",
                "  url: http://localhost:6333",
                "  collection: kuaia_chunks",
                "  idField: id",
                "  vectorField: embedding",
                "  chunkIndexField: chunk_index",
                "  chunkIdMultiplier: 10000").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals(10_000L, config.getSink().getChunkIdMultiplier());
    }

    @Test
    void rejectsInvalidQdrantChunkIdMultiplier() throws Exception {
        Path configPath = tempDir.resolve("invalid-qdrant-chunk-id-multiplier.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-qdrant-chunk-id-multiplier",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "sink:",
                "  type: qdrant",
                "  url: http://localhost:6333",
                "  collection: kuaia_chunks",
                "  idField: id",
                "  vectorField: embedding",
                "  chunkIndexField: chunk_index",
                "  chunkIdMultiplier: 0").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid sink.chunkIdMultiplier: 0", error.getMessage());
    }

    @Test
    void rejectsQdrantChunkIdMultiplierWithoutChunkIndexField() throws Exception {
        Path configPath = tempDir.resolve("qdrant-chunk-id-multiplier-without-field.yaml");
        Files.write(configPath, String.join("\n",
                "name: qdrant-chunk-id-multiplier-without-field",
                "source:",
                "  type: file",
                "  path: data/articles.jsonl",
                "  format: jsonl",
                "sink:",
                "  type: qdrant",
                "  url: http://localhost:6333",
                "  collection: kuaia_chunks",
                "  idField: id",
                "  vectorField: embedding",
                "  chunkIdMultiplier: 10000").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("sink.chunkIdMultiplier requires sink.chunkIndexField", error.getMessage());
    }

    @Test
    void rejectsQdrantChunkPointIdFieldsForFileSink() throws Exception {
        Path configPath = tempDir.resolve("file-sink-chunk-id.yaml");
        Files.write(configPath, String.join("\n",
                "name: file-sink-chunk-id",
                "source:",
                "  type: file",
                "  path: data/users.csv",
                "  format: csv",
                "sink:",
                "  type: file",
                "  path: out/users.csv",
                "  format: csv",
                "  chunkIndexField: chunk_index").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("sink.chunkIndexField is only supported for sink.type: qdrant", error.getMessage());
    }

    @Test
    void rejectsQdrantPayloadFieldsForFileSink() throws Exception {
        Path configPath = tempDir.resolve("file-sink-payload-fields.yaml");
        Files.write(configPath, String.join("\n",
                "name: file-sink-payload-fields",
                "source:",
                "  type: file",
                "  path: data/users.csv",
                "  format: csv",
                "sink:",
                "  type: file",
                "  path: out/users.csv",
                "  format: csv",
                "  payloadFields: [id]").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("sink.payloadFields is only supported for sink.type: qdrant, pgvector, or milvus",
                error.getMessage());
    }

    @Test
    void rejectsInvalidQdrantSinkTimeout() throws Exception {
        Path configPath = tempDir.resolve("invalid-qdrant-sink-timeout.yaml");
        Files.write(configPath, String.join("\n",
                "name: invalid-qdrant-sink-timeout",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "sink:",
                "  type: qdrant",
                "  url: http://localhost:6333",
                "  collection: kuaia_docs",
                "  idField: id",
                "  vectorField: embedding",
                "  timeoutMs: zero").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Invalid sink.timeoutMs: zero", error.getMessage());
    }

    @Test
    void rejectsQdrantTimeoutForFileSink() throws Exception {
        Path configPath = tempDir.resolve("file-sink-timeout.yaml");
        Files.write(configPath, String.join("\n",
                "name: file-sink-timeout",
                "source:",
                "  type: file",
                "  path: data/users.csv",
                "  format: csv",
                "sink:",
                "  type: file",
                "  path: out/users.csv",
                "  format: csv",
                "  timeoutMs: 12000").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("sink.timeoutMs is only supported for sink.type: qdrant, pgvector, or milvus",
                error.getMessage());
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
    void loadsPgvectorSinkConfig() throws Exception {
        Path configPath = tempDir.resolve("pgvector-sink.yaml");
        Files.write(configPath, String.join("\n",
                "name: pgvector-sink",
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
                "  type: pgvector",
                "  url: jdbc:postgresql://localhost:5432/kuaia",
                "  table: document_vectors",
                "  userEnv: KUAIA_POSTGRES_USER",
                "  passwordEnv: KUAIA_POSTGRES_PASSWORD",
                "  idField: id",
                "  vectorField: embedding",
                "  payloadFields: [content]",
                "  timeoutMs: 12000").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("pgvector", config.getSink().getType());
        assertEquals("jdbc:postgresql://localhost:5432/kuaia", config.getSink().getUrl());
        assertEquals("document_vectors", config.getSink().getTable());
        assertEquals("KUAIA_POSTGRES_USER", config.getSink().getUserEnv());
        assertEquals("KUAIA_POSTGRES_PASSWORD", config.getSink().getPasswordEnv());
        assertEquals("id", config.getSink().getIdField());
        assertEquals("embedding", config.getSink().getVectorField());
        assertEquals(java.util.Collections.singletonList("content"), config.getSink().getPayloadFields());
        assertEquals(12000, config.getSink().getTimeoutMs());
    }

    @Test
    void loadsMilvusSinkConfig() throws Exception {
        Path configPath = tempDir.resolve("milvus-sink.yaml");
        Files.write(configPath, String.join("\n",
                "name: milvus-sink",
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
                "  type: milvus",
                "  url: http://localhost:19530",
                "  collection: kuaia_docs",
                "  apiKeyEnv: KUAIA_MILVUS_TOKEN",
                "  idField: id",
                "  vectorField: embedding",
                "  payloadFields: [content]",
                "  timeoutMs: 12000").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("milvus", config.getSink().getType());
        assertEquals("http://localhost:19530", config.getSink().getUrl());
        assertEquals("kuaia_docs", config.getSink().getCollection());
        assertEquals("KUAIA_MILVUS_TOKEN", config.getSink().getApiKeyEnv());
        assertEquals("id", config.getSink().getIdField());
        assertEquals("embedding", config.getSink().getVectorField());
        assertEquals(java.util.Collections.singletonList("content"), config.getSink().getPayloadFields());
        assertEquals(12000, config.getSink().getTimeoutMs());
    }

    @Test
    void rejectsMissingPgvectorTable() throws Exception {
        Path configPath = tempDir.resolve("missing-pgvector-table.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-pgvector-table",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "sink:",
                "  type: pgvector",
                "  url: jdbc:postgresql://localhost:5432/kuaia",
                "  userEnv: KUAIA_POSTGRES_USER",
                "  passwordEnv: KUAIA_POSTGRES_PASSWORD",
                "  idField: id",
                "  vectorField: embedding").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Missing required field: sink.table", error.getMessage());
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
    void rejectsPostgresSourceWithNonPostgresJdbcUrl() throws Exception {
        Path configPath = tempDir.resolve("postgres-with-mysql-url.yaml");
        Files.write(configPath, String.join("\n",
                "name: postgres-with-mysql-url",
                "source:",
                "  type: postgres",
                "  url: jdbc:mysql://localhost:3306/kuaia",
                "  userEnv: KUAIA_POSTGRES_USER",
                "  passwordEnv: KUAIA_POSTGRES_PASSWORD",
                "  query: select id, content from documents order by id",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("source.url for source.type postgres must start with jdbc:postgresql:", error.getMessage());
    }

    @Test
    void loadsMysqlSourceConfig() throws Exception {
        Path configPath = tempDir.resolve("mysql-source.yaml");
        Files.write(configPath, String.join("\n",
                "name: mysql-source",
                "source:",
                "  type: mysql",
                "  url: jdbc:mysql://localhost:3306/kuaia",
                "  userEnv: KUAIA_MYSQL_USER",
                "  passwordEnv: KUAIA_MYSQL_PASSWORD",
                "  query: select id, content from documents order by id",
                "  fetchSize: 256",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("mysql", config.getSource().getType());
        assertEquals("jdbc:mysql://localhost:3306/kuaia", config.getSource().getUrl());
        assertEquals("KUAIA_MYSQL_USER", config.getSource().getUserEnv());
        assertEquals("KUAIA_MYSQL_PASSWORD", config.getSource().getPasswordEnv());
        assertEquals("select id, content from documents order by id", config.getSource().getQuery());
        assertEquals(256, config.getSource().getFetchSize());
    }

    @Test
    void loadsDuckdbSourceConfig() throws Exception {
        Path configPath = tempDir.resolve("duckdb-source.yaml");
        Files.write(configPath, String.join("\n",
                "name: duckdb-source",
                "source:",
                "  type: duckdb",
                "  query: select id, content from read_csv_auto('data/documents.csv') order by id",
                "  fetchSize: 128",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("duckdb", config.getSource().getType());
        assertEquals("jdbc:duckdb:", config.getSource().getUrl());
        assertEquals("select id, content from read_csv_auto('data/documents.csv') order by id",
                config.getSource().getQuery());
        assertEquals(128, config.getSource().getFetchSize());
    }

    @Test
    void rejectsDuckdbCredentials() throws Exception {
        Path configPath = tempDir.resolve("duckdb-credentials.yaml");
        Files.write(configPath, String.join("\n",
                "name: duckdb-credentials",
                "source:",
                "  type: duckdb",
                "  userEnv: KUAIA_DUCKDB_USER",
                "  query: select 1 as id",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("source.userEnv is not supported for source.type: duckdb", error.getMessage());
    }

    @Test
    void loadsDocumentSourceConfigWithDefaultDocumentType() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Path configPath = tempDir.resolve("document-source.yaml");
        Files.write(configPath, String.join("\n",
                "name: document-source",
                "source:",
                "  type: file",
                "  path: docs",
                "  format: document",
                "transforms:",
                "  - type: chunk",
                "    input: content",
                "    output: content",
                "    chunkSize: 100",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        PipelineConfig config = new PipelineConfigLoader().load(configPath);

        assertEquals("file", config.getSource().getType());
        assertEquals(docs.toString(), config.getSource().getPath());
        assertEquals("document", config.getSource().getFormat());
        assertEquals("auto", config.getSource().getDocumentType());
    }

    @Test
    void loadsDocumentSourceConfigWithExplicitDocumentType() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        for (String documentType : new String[]{"auto", "text", "markdown", "pdf"}) {
            Path configPath = tempDir.resolve("document-source-" + documentType + ".yaml");
            Files.write(configPath, String.join("\n",
                    "name: document-source-" + documentType,
                    "source:",
                    "  type: file",
                    "  path: docs",
                    "  format: document",
                    "  documentType: " + documentType,
                    "sink:",
                    "  type: console").getBytes(StandardCharsets.UTF_8));

            PipelineConfig config = new PipelineConfigLoader().load(configPath);

            assertEquals("file", config.getSource().getType());
            assertEquals("document", config.getSource().getFormat());
            assertEquals(documentType, config.getSource().getDocumentType());
        }
    }

    @Test
    void rejectsDocumentDirectorySourceTypeWithMigrationHint() throws Exception {
        Path configPath = tempDir.resolve("document-directory-source.yaml");
        Files.write(configPath, String.join("\n",
                "name: document-directory-source",
                "source:",
                "  type: document-directory",
                "  path: docs",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals(
                "source.type document-directory has been replaced by source.type: file with format: document",
                error.getMessage());
    }

    @Test
    void rejectsMaxRowsPerSplitForDocumentFormat() throws Exception {
        Path configPath = tempDir.resolve("document-source-split.yaml");
        Files.write(configPath, String.join("\n",
                "name: document-source-split",
                "source:",
                "  type: file",
                "  path: docs",
                "  format: document",
                "  maxRowsPerSplit: 2",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("source.maxRowsPerSplit is only supported for file formats csv and jsonl", error.getMessage());
    }

    @Test
    void rejectsDocumentTypeForCsvFormat() throws Exception {
        Path configPath = tempDir.resolve("csv-document-type.yaml");
        Files.write(configPath, String.join("\n",
                "name: csv-document-type",
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "  documentType: auto",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("source.documentType is only supported for source.format: document", error.getMessage());
    }

    @Test
    void rejectsDocumentTypeForNonFileSourceTypes() throws Exception {
        for (String sourceType : new String[]{"duckdb", "postgres", "s3"}) {
            Path configPath = tempDir.resolve(sourceType + "-document-type.yaml");
            Files.write(configPath, String.join("\n",
                    "name: " + sourceType + "-document-type",
                    "source:",
                    "  type: " + sourceType,
                    "  documentType: auto",
                    "sink:",
                    "  type: console").getBytes(StandardCharsets.UTF_8));

            PipelineConfigException error = assertThrows(
                    PipelineConfigException.class,
                    () -> new PipelineConfigLoader().load(configPath));

            assertEquals(
                    "source.documentType is only supported for source.format: document",
                    error.getMessage(),
                    sourceType);
        }
    }

    @Test
    void rejectsUnknownDocumentType() throws Exception {
        Path configPath = tempDir.resolve("unknown-document-type.yaml");
        Files.write(configPath, String.join("\n",
                "name: unknown-document-type",
                "source:",
                "  type: file",
                "  path: docs",
                "  format: document",
                "  documentType: html",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("Unsupported source.documentType: html", error.getMessage());
    }

    @Test
    void rejectsMysqlSourceWithNonMysqlJdbcUrl() throws Exception {
        Path configPath = tempDir.resolve("mysql-with-postgres-url.yaml");
        Files.write(configPath, String.join("\n",
                "name: mysql-with-postgres-url",
                "source:",
                "  type: mysql",
                "  url: jdbc:postgresql://localhost:5432/kuaia",
                "  userEnv: KUAIA_MYSQL_USER",
                "  passwordEnv: KUAIA_MYSQL_PASSWORD",
                "  query: select id, content from documents order by id",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals("source.url for source.type mysql must start with jdbc:mysql:", error.getMessage());
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
    void rejectsMissingMysqlQuery() throws Exception {
        Path configPath = tempDir.resolve("missing-mysql-query.yaml");
        Files.write(configPath, String.join("\n",
                "name: missing-mysql-query",
                "source:",
                "  type: mysql",
                "  url: jdbc:mysql://localhost:3306/kuaia",
                "  userEnv: KUAIA_MYSQL_USER",
                "  passwordEnv: KUAIA_MYSQL_PASSWORD",
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

    private void assertFileSourceFieldRejected(String field, String value, String expectedMessage) throws Exception {
        Path configPath = tempDir.resolve("file-source-" + field + ".yaml");
        Files.write(configPath, String.join("\n",
                "name: file-source-" + field,
                "source:",
                "  type: file",
                "  path: data/documents.csv",
                "  format: csv",
                "  " + field + ": " + value,
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals(expectedMessage, error.getMessage());
    }

    private void assertJdbcSourceFieldRejected(String field, String value, String expectedMessage) throws Exception {
        Path configPath = tempDir.resolve("mysql-source-" + field + ".yaml");
        Files.write(configPath, String.join("\n",
                "name: mysql-source-" + field,
                "source:",
                "  type: mysql",
                "  url: jdbc:mysql://localhost:3306/kuaia",
                "  userEnv: KUAIA_MYSQL_USER",
                "  passwordEnv: KUAIA_MYSQL_PASSWORD",
                "  query: select id, content from documents order by id",
                "  " + field + ": " + value,
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        PipelineConfigException error = assertThrows(
                PipelineConfigException.class,
                () -> new PipelineConfigLoader().load(configPath));

        assertEquals(expectedMessage, error.getMessage());
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
