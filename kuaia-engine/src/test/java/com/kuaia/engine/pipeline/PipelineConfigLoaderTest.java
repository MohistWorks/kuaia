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
