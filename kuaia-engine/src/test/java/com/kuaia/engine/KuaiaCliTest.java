package com.kuaia.engine;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KuaiaCliTest {
    @TempDir
    Path tempDir;

    @Test
    void helpPrintsAvailableCommands() throws Exception {
        CliResult result = run("help");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Usage: kuaia <command>"));
        assertTrue(result.output.contains("run -f PIPELINE"));
        assertTrue(result.output.contains("local-demo"));
        assertTrue(result.output.contains("ai-demo"));
        assertTrue(result.output.contains("recover-demo"));
    }

    @Test
    void unknownCommandReturnsError() throws Exception {
        CliResult result = run("missing");

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Unknown command: missing"));
    }

    @Test
    void localDemoRunsThroughCli() throws Exception {
        CliResult result = run("local-demo");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Starting Local Pipeline..."));
        assertTrue(result.output.contains("[Kuaia] Row: id=1, name=User-1"));
        assertTrue(result.output.contains("Pipeline Finished. rows=10"));
    }

    @Test
    void aiDemoRunsThroughCli() throws Exception {
        CliResult result = run("ai-demo");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Starting AI Vector Demo Pipeline..."));
        assertTrue(result.output.contains("[AI Sink] Row ID: 1, Vector Dim: 4"));
        assertTrue(result.output.contains("AI Vector Demo Finished. rows=10"));
    }

    @Test
    void recoveryDemoShowsRecoveredTaskAfterRestart() throws Exception {
        CliResult result = run("recover-demo", "--state-dir", tempDir.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Recovered schedulable tasks: task-recovering"));
        assertTrue(result.output.contains("task-recovering state=RETRYING checkpoint=7"));
    }

    @Test
    void runExecutesDeclarativeLocalPipeline() throws Exception {
        Path data = tempDir.resolve("users.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2,Bob").getBytes(StandardCharsets.UTF_8));
        Path config = writePipelineConfigWithoutCheckpoint("local-file-to-console", data);

        CliResult result = run("run", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Starting pipeline: local-file-to-console"));
        assertTrue(result.output.contains("[Kuaia] Row: id=1, name=Alice"));
        assertTrue(result.output.contains("[Kuaia] Row: id=2, name=Bob"));
        assertTrue(result.output.contains("Pipeline Finished. rows=2"));
    }

    @Test
    void runPersistsCheckpointWhenConfigured() throws Exception {
        Path data = tempDir.resolve("checkpointed.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2,Bob").getBytes(StandardCharsets.UTF_8));
        Path stateDir = tempDir.resolve("checkpoint-state");
        Path config = writePipelineConfig("checkpointed", data, stateDir);

        CliResult result = run("run", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Pipeline Finished. rows=2"));
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-checkpointed");
            assertNotNull(record);
            assertEquals(TaskState.COMPLETED, record.getState());
            assertEquals(2L, record.getLastCheckpointSeq());
        }
    }

    @Test
    void runResumesFromCheckpointAfterCsvFailure() throws Exception {
        Path data = tempDir.resolve("resume.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2,Bob",
                "3").getBytes(StandardCharsets.UTF_8));
        Path stateDir = tempDir.resolve("resume-state");
        Path config = writePipelineConfig("resume", data, stateDir);

        CliResult first = run("run", "-f", config.toString());

        assertEquals(1, first.exitCode);
        assertTrue(first.output.contains("[Kuaia] Row: id=1, name=Alice"));
        assertTrue(first.output.contains("[Kuaia] Row: id=2, name=Bob"));
        assertTrue(first.output.contains("Invalid CSV row at line 4: expected 2 columns but found 1"));
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-resume");
            assertNotNull(record);
            assertEquals(TaskState.RUNNING, record.getState());
            assertEquals(2L, record.getLastCheckpointSeq());
        }

        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2,Bob",
                "3,Carol").getBytes(StandardCharsets.UTF_8));

        CliResult second = run("run", "-f", config.toString());

        assertEquals(0, second.exitCode);
        assertFalse(second.output.contains("[Kuaia] Row: id=1, name=Alice"));
        assertFalse(second.output.contains("[Kuaia] Row: id=2, name=Bob"));
        assertTrue(second.output.contains("[Kuaia] Row: id=3, name=Carol"));
        assertTrue(second.output.contains("Pipeline Finished. rows=1"));
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-resume");
            assertNotNull(record);
            assertEquals(TaskState.COMPLETED, record.getState());
            assertEquals(3L, record.getLastCheckpointSeq());
        }
    }

    @Test
    void runAppliesSelectAndRenameTransforms() throws Exception {
        Path data = tempDir.resolve("users-transform.csv");
        Files.write(data, String.join("\n",
                "id,name,email",
                "1,Alice,alice@example.test",
                "2,Bob,bob@example.test").getBytes(StandardCharsets.UTF_8));
        Path config = writeTransformPipelineConfig("transform-users", data, null);

        CliResult result = run("run", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("[Kuaia] Row: id=1, user_name=Alice"));
        assertTrue(result.output.contains("[Kuaia] Row: id=2, user_name=Bob"));
        assertFalse(result.output.contains("email="));
    }

    @Test
    void runReportsUnsupportedTransformType() throws Exception {
        Path data = tempDir.resolve("unsupported-transform.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice").getBytes(StandardCharsets.UTF_8));
        Path config = tempDir.resolve("unsupported-transform.yaml");
        Files.write(config, String.join("\n",
                "name: unsupported-transform",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: missing",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Unsupported transform.type: missing"));
    }

    @Test
    void runReportsUnknownSelectedTransformField() throws Exception {
        Path data = tempDir.resolve("unknown-transform-field.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice").getBytes(StandardCharsets.UTF_8));
        Path config = tempDir.resolve("unknown-transform-field.yaml");
        Files.write(config, String.join("\n",
                "name: unknown-transform-field",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: select",
                "    fields: [id, missing]",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Unknown transform field: missing"));
    }

    @Test
    void runResumesCheckpointedTransformPipelineAfterCsvFailure() throws Exception {
        Path data = tempDir.resolve("resume-transform.csv");
        Files.write(data, String.join("\n",
                "id,name,email",
                "1,Alice,alice@example.test",
                "2,Bob,bob@example.test",
                "3,Carol").getBytes(StandardCharsets.UTF_8));
        Path stateDir = tempDir.resolve("resume-transform-state");
        Path config = writeTransformPipelineConfig("resume-transform", data, stateDir);

        CliResult first = run("run", "-f", config.toString());

        assertEquals(1, first.exitCode);
        assertTrue(first.output.contains("[Kuaia] Row: id=1, user_name=Alice"));
        assertTrue(first.output.contains("[Kuaia] Row: id=2, user_name=Bob"));
        assertTrue(first.output.contains("Invalid CSV row at line 4: expected 3 columns but found 2"));
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-resume-transform");
            assertNotNull(record);
            assertEquals(TaskState.RUNNING, record.getState());
            assertEquals(2L, record.getLastCheckpointSeq());
        }

        Files.write(data, String.join("\n",
                "id,name,email",
                "1,Alice,alice@example.test",
                "2,Bob,bob@example.test",
                "3,Carol,carol@example.test").getBytes(StandardCharsets.UTF_8));

        CliResult second = run("run", "-f", config.toString());

        assertEquals(0, second.exitCode);
        assertFalse(second.output.contains("[Kuaia] Row: id=1, user_name=Alice"));
        assertFalse(second.output.contains("[Kuaia] Row: id=2, user_name=Bob"));
        assertTrue(second.output.contains("[Kuaia] Row: id=3, user_name=Carol"));
        assertFalse(second.output.contains("email="));
        assertTrue(second.output.contains("Pipeline Finished. rows=1"));
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-resume-transform");
            assertNotNull(record);
            assertEquals(TaskState.COMPLETED, record.getState());
            assertEquals(3L, record.getLastCheckpointSeq());
        }
    }

    @Test
    void runRequiresConfigPath() throws Exception {
        CliResult result = run("run");

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("run requires -f <pipeline.yaml>"));
    }

    @Test
    void runReportsMissingConfigFile() throws Exception {
        CliResult result = run("run", "-f", tempDir.resolve("missing.yaml").toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Pipeline config not found:"));
    }

    @Test
    void runReportsMissingRequiredConfigField() throws Exception {
        Path config = tempDir.resolve("missing-source-path.yaml");
        Files.write(config, String.join("\n",
                "name: broken",
                "source:",
                "  type: file",
                "  format: csv",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Missing required field: source.path"));
    }

    @Test
    void runReportsMalformedCsvRows() throws Exception {
        Path data = tempDir.resolve("bad.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2").getBytes(StandardCharsets.UTF_8));
        Path config = tempDir.resolve("bad.yaml");
        Files.write(config, String.join("\n",
                "name: bad",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Invalid CSV row at line 3: expected 2 columns but found 1"));
    }

    private CliResult run(String... args) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8.name());
        int exitCode = KuaiaCli.run(args, out);
        out.flush();
        return new CliResult(exitCode, bytes.toString(StandardCharsets.UTF_8.name()));
    }

    private Path writePipelineConfig(String name, Path data, Path stateDir) throws Exception {
        Path config = tempDir.resolve(name + ".yaml");
        Files.write(config, String.join("\n",
                "name: " + name,
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "sink:",
                "  type: console",
                "checkpoint:",
                "  stateDir: " + stateDir).getBytes(StandardCharsets.UTF_8));
        return config;
    }

    private Path writePipelineConfigWithoutCheckpoint(String name, Path data) throws Exception {
        Path config = tempDir.resolve(name + ".yaml");
        Files.write(config, String.join("\n",
                "name: " + name,
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));
        return config;
    }

    private Path writeTransformPipelineConfig(String name, Path data, Path stateDir) throws Exception {
        Path config = tempDir.resolve(name + ".yaml");
        StringBuilder yaml = new StringBuilder();
        yaml.append(String.join("\n",
                "name: " + name,
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: select",
                "    fields: [id, name]",
                "  - type: rename",
                "    from: name",
                "    to: user_name",
                "sink:",
                "  type: console"));
        if (stateDir != null) {
            yaml.append("\n")
                    .append("checkpoint:\n")
                    .append("  stateDir: ")
                    .append(stateDir);
        }
        Files.write(config, yaml.toString().getBytes(StandardCharsets.UTF_8));
        return config;
    }

    private static class CliResult {
        private final int exitCode;
        private final String output;

        private CliResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
