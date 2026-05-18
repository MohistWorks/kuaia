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
        assertTrue(result.output.contains("validate -f PIPELINE"));
        assertTrue(result.output.contains("local-demo"));
        assertTrue(result.output.contains("ai-demo"));
        assertTrue(result.output.contains("recover-demo"));
        assertTrue(result.output.contains("examples"));
        assertTrue(result.output.contains("benchmark"));
        assertTrue(result.output.contains("Examples:"));
        assertTrue(result.output.contains("kuaia run -f examples/local-file-to-file.yaml"));
        assertTrue(result.output.contains("kuaia run -f examples/local-file-to-vector.yaml"));
    }

    @Test
    void validateChecksFilePipelineWithoutRunningIt() throws Exception {
        Path data = tempDir.resolve("validate-users.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2,Bob").getBytes(StandardCharsets.UTF_8));
        Path output = tempDir.resolve("out/validate-users.csv");
        Path stateDir = tempDir.resolve("validate-state");
        Path config = writeFileSinkPipelineConfig("validate-users", data, output, "overwrite", stateDir);

        CliResult result = run("validate", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Pipeline valid: validate-users"));
        assertTrue(result.output.contains("Source: file fields=2"));
        assertTrue(result.output.contains("Transforms: 0"));
        assertTrue(result.output.contains("Sink: file"));
        assertFalse(Files.exists(output));
        assertFalse(Files.exists(stateDir));
        assertFalse(result.output.contains("Starting pipeline:"));
        assertFalse(result.output.contains("Run Summary:"));
    }

    @Test
    void validateReportsTransformFieldErrorsWithoutRunningPipeline() throws Exception {
        Path data = tempDir.resolve("validate-unknown-transform-field.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice").getBytes(StandardCharsets.UTF_8));
        Path config = tempDir.resolve("validate-unknown-transform-field.yaml");
        Files.write(config, String.join("\n",
                "name: validate-unknown-transform-field",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: select",
                "    fields: [id, missing]",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("validate", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Transform stage failed: Unknown transform field: missing"));
        assertFalse(result.output.contains("Starting pipeline:"));
        assertFalse(result.output.contains("Run Summary:"));
    }

    @Test
    void validateDoesNotConnectToPostgres() throws Exception {
        Path config = tempDir.resolve("validate-postgres.yaml");
        Files.write(config, String.join("\n",
                "name: validate-postgres",
                "source:",
                "  type: postgres",
                "  url: jdbc:postgresql://localhost:5432/kuaia",
                "  userEnv: PGUSER",
                "  passwordEnv: PGPASSWORD",
                "  query: SELECT id, content FROM documents",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("validate", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Pipeline valid: validate-postgres"));
        assertTrue(result.output.contains("Source: postgres fields=deferred"));
        assertTrue(result.output.contains("Transform and sink row-type checks deferred for source.type: postgres"));
    }

    @Test
    void validateDoesNotConnectToMysql() throws Exception {
        Path config = tempDir.resolve("validate-mysql.yaml");
        Files.write(config, String.join("\n",
                "name: validate-mysql",
                "source:",
                "  type: mysql",
                "  url: jdbc:mysql://localhost:3306/kuaia",
                "  userEnv: MYSQL_USER",
                "  passwordEnv: MYSQL_PASSWORD",
                "  query: SELECT id, content FROM documents",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("validate", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Pipeline valid: validate-mysql"));
        assertTrue(result.output.contains("Source: mysql fields=deferred"));
        assertTrue(result.output.contains("Transform and sink row-type checks deferred for source.type: mysql"));
    }

    @Test
    void validateDoesNotConnectToDuckdb() throws Exception {
        Path config = tempDir.resolve("validate-duckdb.yaml");
        Files.write(config, String.join("\n",
                "name: validate-duckdb",
                "source:",
                "  type: duckdb",
                "  query: SELECT id, content FROM read_csv_auto('missing.csv')",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("validate", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Pipeline valid: validate-duckdb"));
        assertTrue(result.output.contains("Source: duckdb fields=deferred"));
        assertTrue(result.output.contains("Transform and sink row-type checks deferred for source.type: duckdb"));
    }

    @Test
    void validateRejectsMysqlSourceWithPostgresJdbcUrl() throws Exception {
        Path config = tempDir.resolve("validate-mysql-url-mismatch.yaml");
        Files.write(config, String.join("\n",
                "name: validate-mysql-url-mismatch",
                "source:",
                "  type: mysql",
                "  url: jdbc:postgresql://localhost:5432/kuaia",
                "  userEnv: MYSQL_USER",
                "  passwordEnv: MYSQL_PASSWORD",
                "  query: SELECT id, content FROM documents",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("validate", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("source.url for source.type mysql must start with jdbc:mysql:"));
        assertFalse(result.output.contains("Pipeline valid:"));
    }

    @Test
    void validateRequiresConfigPath() throws Exception {
        CliResult result = run("validate");

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("validate requires -f <pipeline.yaml>"));
    }

    @Test
    void examplesPrintsRecommendedPublicMvpPaths() throws Exception {
        CliResult result = run("examples");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Recommended no-service smoke:"));
        assertTrue(result.output.contains("make public-mvp-smoke"));
        assertTrue(result.output.contains("Recommended preflight:"));
        assertTrue(result.output.contains("kuaia validate -f examples/local-file-to-file.yaml"));
        assertTrue(result.output.contains("No external services:"));
        assertTrue(result.output.contains("examples/local-file-to-file.yaml"));
        assertTrue(result.output.contains("examples/local-quoted-csv-to-file.yaml"));
        assertTrue(result.output.contains("examples/local-jsonl-to-file.yaml"));
        assertTrue(result.output.contains("examples/local-file-to-vector.yaml"));
        assertTrue(result.output.contains("examples/local-jsonl-to-vector.yaml"));
        assertTrue(result.output.contains("examples/local-jsonl-chunk-to-vector.yaml"));
        assertTrue(result.output.contains("examples/local-faq-jsonl-to-vector.yaml"));
        assertTrue(result.output.contains("examples/local-file-skip-bad-records.yaml"));
        assertTrue(result.output.contains("Common RAG flows:"));
        assertTrue(result.output.contains("Document directory to Qdrant: kuaia run -f examples/document-directory-to-qdrant.yaml"));
        assertTrue(result.output.contains("FAQ import: kuaia run -f examples/local-faq-jsonl-to-vector.yaml"));
        assertTrue(result.output.contains("DuckDB to Qdrant: kuaia run -f examples/duckdb-csv-to-qdrant.yaml"));
        assertTrue(result.output.contains("Postgres to Qdrant: kuaia run -f examples/postgres-to-qdrant.yaml"));
        assertTrue(result.output.contains("MySQL to Qdrant: kuaia run -f examples/mysql-to-qdrant.yaml"));
        assertTrue(result.output.contains("External service examples:"));
        assertTrue(result.output.contains("examples/local-file-to-openai-compatible-vector.yaml"));
        assertTrue(result.output.contains("examples/local-file-to-qdrant.yaml"));
        assertTrue(result.output.contains("examples/local-jsonl-chunk-to-qdrant.yaml"));
        assertTrue(result.output.contains("examples/document-directory-to-qdrant.yaml"));
        assertTrue(result.output.contains("examples/duckdb-csv-to-qdrant.yaml"));
        assertTrue(result.output.contains("examples/postgres-to-qdrant.yaml"));
        assertTrue(result.output.contains("examples/mysql-to-qdrant.yaml"));
    }

    @Test
    void benchmarkRunsLocalBatchBenchmarkAndWritesJson() throws Exception {
        Path output = tempDir.resolve("benchmark/local-pipeline-batch.json");

        CliResult result = run(
                "benchmark",
                "--rows", "6",
                "--max-rows-per-split", "4",
                "--output", output.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Benchmark: local batch pipeline"));
        assertTrue(result.output.contains("rows=6"));
        assertTrue(result.output.contains("maxRowsPerSplit=4"));
        assertTrue(result.output.contains("batchSize=1 rowsWritten=6 sourceSplits=2 sinkBatches=6"));
        assertTrue(result.output.contains("batchSize=8 rowsWritten=6 sourceSplits=2 sinkBatches=2"));
        assertTrue(result.output.contains("output=" + output));
        assertTrue(Files.exists(output));
        String json = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"rowCount\":6"));
        assertTrue(json.contains("\"batchSize\":1"));
        assertTrue(json.contains("\"batchSize\":8"));
        assertTrue(json.contains("\"sourceSplits\":2"));
        assertTrue(json.contains("\"sinkBatches\":2"));
    }

    @Test
    void benchmarkRejectsInvalidRows() throws Exception {
        CliResult result = run("benchmark", "--rows", "0");

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("benchmark --rows must be greater than zero"));
    }

    @Test
    void benchmarkAcceptsCustomBatchSizes() throws Exception {
        Path output = tempDir.resolve("benchmark/custom-batches.json");

        CliResult result = run(
                "benchmark",
                "--rows", "6",
                "--batch-sizes", "2,3",
                "--output", output.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("batchSizes=2,3"));
        assertFalse(result.output.contains("batchSize=1 rowsWritten="));
        assertTrue(result.output.contains("batchSize=2 rowsWritten=6 sourceSplits=1 sinkBatches=3"));
        assertTrue(result.output.contains("batchSize=3 rowsWritten=6 sourceSplits=1 sinkBatches=2"));
        String json = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
        assertFalse(json.contains("\"batchSize\":1"));
        assertTrue(json.contains("\"batchSize\":2"));
        assertTrue(json.contains("\"batchSize\":3"));
    }

    @Test
    void benchmarkRejectsInvalidBatchSizes() throws Exception {
        CliResult result = run("benchmark", "--batch-sizes", "2,0");

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("benchmark --batch-sizes values must be greater than zero"));
    }

    @Test
    void benchmarkWritesCsvWhenConfigured() throws Exception {
        Path output = tempDir.resolve("benchmark/custom-batches.csv");

        CliResult result = run(
                "benchmark",
                "--rows", "6",
                "--batch-sizes", "2,3",
                "--format", "csv",
                "--output", output.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("format=csv"));
        assertTrue(result.output.contains("output=" + output));
        String csv = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
        assertTrue(csv.startsWith("rowCount,batchSize,durationMs,rowsPerSecond,"));
        assertTrue(csv.contains("6,2,"));
        assertTrue(csv.contains(",3,0,6,3,6,"));
        assertTrue(csv.contains("6,3,"));
        assertFalse(csv.contains("\"batchSize\""));
    }

    @Test
    void benchmarkRejectsInvalidFormat() throws Exception {
        CliResult result = run("benchmark", "--format", "xml");

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("benchmark --format must be json or csv: xml"));
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
    void runWritesDeclarativePipelineToCsvFile() throws Exception {
        Path data = tempDir.resolve("users-file-sink.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2,Bob").getBytes(StandardCharsets.UTF_8));
        Path output = tempDir.resolve("out/users.csv");
        Path config = writeFileSinkPipelineConfig("local-file-to-file", data, output, "overwrite");

        CliResult result = run("run", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Starting pipeline: local-file-to-file"));
        assertTrue(result.output.contains("Pipeline Finished. rows=2"));
        assertTrue(result.output.contains(
                "Run Summary: rowsRead=2 rowsWritten=2 rowsFailed=0 rowsSkipped=0 checkpointSeq=2 taskState=COMPLETED sourceSplits=1 sinkBatches=2 durationMs="));
        assertEquals(String.join("\n",
                        "id,name",
                        "1,Alice",
                        "2,Bob"),
                String.join("\n", Files.readAllLines(output, StandardCharsets.UTF_8)));
    }

    @Test
    void runReadsDuckdbCsvQueryToFile() throws Exception {
        Path data = tempDir.resolve("duckdb-documents.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta").getBytes(StandardCharsets.UTF_8));
        Path output = tempDir.resolve("out/duckdb-documents.csv");
        Path config = tempDir.resolve("duckdb-to-file.yaml");
        Files.write(config, String.join("\n",
                "name: duckdb-to-file",
                "source:",
                "  type: duckdb",
                "  query: select id, content from read_csv_auto('" + sqlString(data) + "') order by id",
                "sink:",
                "  type: file",
                "  path: " + output,
                "  format: csv",
                "  mode: overwrite").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(0, result.exitCode, result.output);
        assertTrue(result.output.contains("Starting pipeline: duckdb-to-file"));
        assertTrue(result.output.contains("Pipeline Finished. rows=2"));
        assertTrue(result.output.contains(
                "Run Summary: rowsRead=2 rowsWritten=2 rowsFailed=0 rowsSkipped=0 checkpointSeq=2 taskState=COMPLETED sourceSplits=1 sinkBatches=2 durationMs="));
        assertEquals(String.join("\n",
                        "id,content",
                        "1,Alpha",
                        "2,Beta"),
                String.join("\n", Files.readAllLines(output, StandardCharsets.UTF_8)));
    }

    @Test
    void runReadsDocumentDirectoryToJsonlFile() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs.resolve("nested"));
        Files.write(docs.resolve("intro.md"), "Intro document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("nested/guide.txt"), "Guide document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("image.png"), "ignored".getBytes(StandardCharsets.UTF_8));
        Path output = tempDir.resolve("out/documents.jsonl");
        Path config = tempDir.resolve("document-directory-to-file.yaml");
        Files.write(config, String.join("\n",
                "name: document-directory-to-file",
                "source:",
                "  type: document-directory",
                "  path: " + docs,
                "sink:",
                "  type: file",
                "  path: " + output,
                "  format: jsonl",
                "  mode: overwrite").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(0, result.exitCode, result.output);
        assertTrue(result.output.contains("Starting pipeline: document-directory-to-file"));
        assertTrue(result.output.contains("Pipeline Finished. rows=2"));
        assertTrue(result.output.contains(
                "Run Summary: rowsRead=2 rowsWritten=2 rowsFailed=0 rowsSkipped=0 checkpointSeq=2 taskState=COMPLETED sourceSplits=1 sinkBatches=2 durationMs="));
        assertEquals(String.join("\n",
                        "{\"id\":1,\"path\":\"intro.md\",\"content\":\"Intro document\"}",
                        "{\"id\":2,\"path\":\"nested/guide.txt\",\"content\":\"Guide document\"}"),
                String.join("\n", Files.readAllLines(output, StandardCharsets.UTF_8)));
    }

    @Test
    void runWritesJsonSummaryWhenConfigured() throws Exception {
        Path data = tempDir.resolve("users-summary.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2,Bob").getBytes(StandardCharsets.UTF_8));
        Path output = tempDir.resolve("out/summary-users.csv");
        Path summaryJson = tempDir.resolve("summaries/local-file-to-file.json");
        Path config = writeFileSinkPipelineConfig("local-file-to-file", data, output, "overwrite");

        CliResult result = run(
                "run",
                "-f", config.toString(),
                "--summary-json", summaryJson.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Run Summary JSON: " + summaryJson));
        assertTrue(Files.exists(summaryJson));
        String json = new String(Files.readAllBytes(summaryJson), StandardCharsets.UTF_8);
        assertTrue(json.contains("\"pipelineName\":\"local-file-to-file\""));
        assertTrue(json.contains("\"rowsRead\":2"));
        assertTrue(json.contains("\"rowsWritten\":2"));
        assertTrue(json.contains("\"rowsFailed\":0"));
        assertTrue(json.contains("\"rowsSkipped\":0"));
        assertTrue(json.contains("\"checkpointSeq\":2"));
        assertTrue(json.contains("\"taskState\":\"COMPLETED\""));
        assertTrue(json.contains("\"sourceSplits\":1"));
        assertTrue(json.contains("\"sinkBatches\":2"));
        assertTrue(json.contains("\"durationMs\":"));
    }

    @Test
    void runWritesDeclarativePipelineToJsonlFile() throws Exception {
        Path data = tempDir.resolve("documents-file-sink.jsonl");
        Files.write(data, String.join("\n",
                "{\"id\":1,\"content\":\"  Alpha  \"}",
                "{\"id\":2,\"content\":\"   \"}",
                "{\"id\":3,\"content\":\"Hi\"}").getBytes(StandardCharsets.UTF_8));
        Path output = tempDir.resolve("out/documents.jsonl");
        Path config = tempDir.resolve("local-jsonl-to-file.yaml");
        Files.write(config, String.join("\n",
                "name: local-jsonl-to-file",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: jsonl",
                "transforms:",
                "  - type: trim",
                "    field: content",
                "  - type: filter",
                "    field: content",
                "    op: not-empty",
                "  - type: filter",
                "    field: content",
                "    op: min-length",
                "    minLength: 5",
                "sink:",
                "  type: file",
                "  path: " + output,
                "  format: jsonl",
                "  mode: overwrite").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Starting pipeline: local-jsonl-to-file"));
        assertTrue(result.output.contains("Pipeline Finished. rows=1"));
        assertEquals(
                java.util.Collections.singletonList("{\"id\":1,\"content\":\"Alpha\"}"),
                Files.readAllLines(output, StandardCharsets.UTF_8));
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
    void runDoesNotOverwriteCompletedCheckpointedFileSink() throws Exception {
        Path data = tempDir.resolve("checkpointed-file-sink.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2,Bob").getBytes(StandardCharsets.UTF_8));
        Path output = tempDir.resolve("out/checkpointed-users.csv");
        Path stateDir = tempDir.resolve("checkpointed-file-sink-state");
        Path config = writeFileSinkPipelineConfig("checkpointed-file-sink", data, output, "overwrite", stateDir);

        CliResult first = run("run", "-f", config.toString());
        CliResult second = run("run", "-f", config.toString());

        assertEquals(0, first.exitCode);
        assertEquals(0, second.exitCode);
        assertTrue(second.output.contains("Pipeline Finished. rows=0 checkpoint=2 state=COMPLETED"));
        assertTrue(second.output.contains(
                "Run Summary: rowsRead=0 rowsWritten=0 rowsFailed=0 rowsSkipped=2 checkpointSeq=2 taskState=COMPLETED sourceSplits=0 sinkBatches=0 durationMs="));
        assertEquals(String.join("\n",
                        "id,name",
                        "1,Alice",
                        "2,Bob"),
                String.join("\n", Files.readAllLines(output, StandardCharsets.UTF_8)));
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
    void runKeepsTransformDefinitionErrorsFatalUnderSkipPolicy() throws Exception {
        Path data = tempDir.resolve("skip-policy-unknown-transform-field.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice").getBytes(StandardCharsets.UTF_8));
        Path config = tempDir.resolve("skip-policy-unknown-transform-field.yaml");
        Files.write(config, String.join("\n",
                "name: skip-policy-unknown-transform-field",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: select",
                "    fields: [id, missing]",
                "sink:",
                "  type: console",
                "errorPolicy:",
                "  mode: skip-bad-records").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Transform stage failed: Unknown transform field: missing"));
        assertFalse(result.output.contains("Run Summary:"));
    }

    @Test
    void runReportsDuplicateSelectedTransformField() throws Exception {
        Path data = tempDir.resolve("duplicate-transform-field.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice").getBytes(StandardCharsets.UTF_8));
        Path config = tempDir.resolve("duplicate-transform-field.yaml");
        Files.write(config, String.join("\n",
                "name: duplicate-transform-field",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: select",
                "    fields: [id, id]",
                "sink:",
                "  type: console").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Duplicate transform field: id"));
    }

    @Test
    void runReportsMockVectorSinkWithoutEmbeddingField() throws Exception {
        Path data = tempDir.resolve("vector-without-embedding.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha").getBytes(StandardCharsets.UTF_8));
        Path config = tempDir.resolve("vector-without-embedding.yaml");
        Files.write(config, String.join("\n",
                "name: vector-without-embedding",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: select",
                "    fields: [id, content]",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Sink stage failed: Mock vector sink requires VECTOR field: embedding"));
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
    void runExecutesDeclarativeAiVectorPipeline() throws Exception {
        Path data = tempDir.resolve("documents.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta").getBytes(StandardCharsets.UTF_8));
        Path config = writeAiVectorPipelineConfig("vector-documents", data, null, "content", "embedding");

        CliResult result = run("run", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("[AI Sink] Row ID: 1, Vector Dim: 4, First Val: 5.0000"));
        assertTrue(result.output.contains("[AI Sink] Row ID: 2, Vector Dim: 4, First Val: 4.0000"));
    }

    @Test
    void runExecutesGenericMockEmbeddingPipeline() throws Exception {
        Path data = tempDir.resolve("generic-embedding-documents.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta").getBytes(StandardCharsets.UTF_8));
        Path config = tempDir.resolve("generic-embedding-documents.yaml");
        Files.write(config, String.join("\n",
                "name: generic-embedding-documents",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: select",
                "    fields: [id, content]",
                "  - type: embedding",
                "    provider: mock",
                "    input: content",
                "    output: embedding",
                "    dimensions: 4",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("[AI Sink] Row ID: 1, Vector Dim: 4, First Val: 5.0000"));
        assertTrue(result.output.contains("[AI Sink] Row ID: 2, Vector Dim: 4, First Val: 4.0000"));
    }

    @Test
    void runResumesCheckpointedAiVectorPipelineAfterCsvFailure() throws Exception {
        Path data = tempDir.resolve("resume-vector.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta",
                "3").getBytes(StandardCharsets.UTF_8));
        Path stateDir = tempDir.resolve("resume-vector-state");
        Path config = writeAiVectorPipelineConfig("resume-vector", data, stateDir, "content", "embedding");

        CliResult first = run("run", "-f", config.toString());

        assertEquals(1, first.exitCode);
        assertTrue(first.output.contains("[AI Sink] Row ID: 1, Vector Dim: 4, First Val: 5.0000"));
        assertTrue(first.output.contains("[AI Sink] Row ID: 2, Vector Dim: 4, First Val: 4.0000"));
        assertTrue(first.output.contains("Invalid CSV row at line 4: expected 2 columns but found 1"));
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-resume-vector");
            assertNotNull(record);
            assertEquals(TaskState.RUNNING, record.getState());
            assertEquals(2L, record.getLastCheckpointSeq());
        }

        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta",
                "3,Gamma").getBytes(StandardCharsets.UTF_8));

        CliResult second = run("run", "-f", config.toString());

        assertEquals(0, second.exitCode);
        assertFalse(second.output.contains("[AI Sink] Row ID: 1"));
        assertFalse(second.output.contains("[AI Sink] Row ID: 2"));
        assertTrue(second.output.contains("[AI Sink] Row ID: 3, Vector Dim: 4, First Val: 5.0000"));
        assertTrue(second.output.contains("Pipeline Finished. rows=1"));
        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            TaskRecord record = store.getTask("local-pipeline-resume-vector");
            assertNotNull(record);
            assertEquals(TaskState.COMPLETED, record.getState());
            assertEquals(3L, record.getLastCheckpointSeq());
        }
    }

    @Test
    void runReportsUnknownMockEmbeddingInputField() throws Exception {
        Path data = tempDir.resolve("missing-embedding-input.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha").getBytes(StandardCharsets.UTF_8));
        Path config = writeAiVectorPipelineConfig("missing-embedding-input", data, null, "missing", "embedding");

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Unknown transform field: missing"));
    }

    @Test
    void runReportsNonStringMockEmbeddingInputField() throws Exception {
        Path data = tempDir.resolve("non-string-embedding-input.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha").getBytes(StandardCharsets.UTF_8));
        Path config = writeAiVectorPipelineConfig("non-string-embedding-input", data, null, "id", "embedding");

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Transform field must be STRING: id"));
    }

    @Test
    void runReportsDuplicateMockEmbeddingOutputField() throws Exception {
        Path data = tempDir.resolve("duplicate-embedding-output.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha").getBytes(StandardCharsets.UTF_8));
        Path config = writeAiVectorPipelineConfig("duplicate-embedding-output", data, null, "content", "content");

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Duplicate transform field: content"));
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
    void runReportsPostgresSourceSplitConfigAsUnsupported() throws Exception {
        Path config = tempDir.resolve("postgres-source-split.yaml");
        Files.write(config, String.join("\n",
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

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("source.maxRowsPerSplit is only supported for source.type: file"));
        assertFalse(result.output.contains("Run Summary:"));
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
        assertTrue(result.output.contains(
                "Source stage failed: Invalid CSV row at line 3: expected 2 columns but found 1"));
    }

    @Test
    void runKeepsSinkIoErrorsFatalUnderSkipPolicy() throws Exception {
        Path data = tempDir.resolve("sink-io-error.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice").getBytes(StandardCharsets.UTF_8));
        Path outputDirectory = tempDir.resolve("existing-output-directory");
        Files.createDirectories(outputDirectory);
        Path config = tempDir.resolve("sink-io-error.yaml");
        Files.write(config, String.join("\n",
                "name: sink-io-error",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "sink:",
                "  type: file",
                "  path: " + outputDirectory,
                "  format: csv",
                "  mode: overwrite",
                "errorPolicy:",
                "  mode: skip-bad-records").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(1, result.exitCode);
        assertTrue(result.output.contains("Sink stage failed:"));
        assertFalse(result.output.contains("Run Summary:"));
    }

    @Test
    void runSkipsMalformedCsvRowsWhenConfigured() throws Exception {
        Path data = tempDir.resolve("skip-bad-records.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2",
                "3,Carol").getBytes(StandardCharsets.UTF_8));
        Path config = tempDir.resolve("skip-bad-records.yaml");
        Files.write(config, String.join("\n",
                "name: skip-bad-records",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "sink:",
                "  type: console",
                "errorPolicy:",
                "  mode: skip-bad-records").getBytes(StandardCharsets.UTF_8));

        CliResult result = run("run", "-f", config.toString());

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("[Kuaia] Row: id=1, name=Alice"));
        assertTrue(result.output.contains("[Kuaia] Row: id=3, name=Carol"));
        assertTrue(result.output.contains(
                "Skipped bad record seq=2 error=Invalid CSV row at line 3: expected 2 columns but found 1"));
        assertTrue(result.output.contains(
                "Run Summary: rowsRead=3 rowsWritten=2 rowsFailed=1 rowsSkipped=0 checkpointSeq=3 taskState=COMPLETED"));
    }

    @Test
    void runCheckpointsSkippedMalformedCsvRows() throws Exception {
        Path data = tempDir.resolve("checkpointed-skip-bad-records.csv");
        Files.write(data, String.join("\n",
                "id,name",
                "1,Alice",
                "2",
                "3,Carol").getBytes(StandardCharsets.UTF_8));
        Path stateDir = tempDir.resolve("checkpointed-skip-state");
        Path config = tempDir.resolve("checkpointed-skip-bad-records.yaml");
        Files.write(config, String.join("\n",
                "name: checkpointed-skip-bad-records",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "sink:",
                "  type: console",
                "errorPolicy:",
                "  mode: skip-bad-records",
                "checkpoint:",
                "  stateDir: " + stateDir).getBytes(StandardCharsets.UTF_8));

        CliResult first = run("run", "-f", config.toString());
        CliResult second = run("run", "-f", config.toString());

        assertEquals(0, first.exitCode);
        assertTrue(first.output.contains("Skipped bad record seq=2 error=Invalid CSV row at line 3: expected 2 columns but found 1"));
        assertTrue(first.output.contains(
                "Run Summary: rowsRead=3 rowsWritten=2 rowsFailed=1 rowsSkipped=0 checkpointSeq=3 taskState=COMPLETED"));
        assertEquals(0, second.exitCode);
        assertTrue(second.output.contains("Pipeline Finished. rows=0 checkpoint=3 state=COMPLETED"));
        assertTrue(second.output.contains(
                "Run Summary: rowsRead=0 rowsWritten=0 rowsFailed=0 rowsSkipped=3 checkpointSeq=3 taskState=COMPLETED"));
        assertFalse(second.output.contains("[Kuaia] Row: id=1, name=Alice"));
        assertFalse(second.output.contains("[Kuaia] Row: id=3, name=Carol"));
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

    private String sqlString(Path path) {
        return path.toAbsolutePath().normalize().toString().replace("'", "''");
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

    private Path writeFileSinkPipelineConfig(String name, Path data, Path output, String mode) throws Exception {
        return writeFileSinkPipelineConfig(name, data, output, mode, null);
    }

    private Path writeFileSinkPipelineConfig(
            String name,
            Path data,
            Path output,
            String mode,
            Path stateDir) throws Exception {
        Path config = tempDir.resolve(name + ".yaml");
        StringBuilder yaml = new StringBuilder();
        yaml.append(String.join("\n",
                "name: " + name,
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "sink:",
                "  type: file",
                "  path: " + output,
                "  format: csv",
                "  mode: " + mode));
        if (stateDir != null) {
            yaml.append("\n")
                    .append("checkpoint:\n")
                    .append("  stateDir: ")
                    .append(stateDir);
        }
        Files.write(config, yaml.toString().getBytes(StandardCharsets.UTF_8));
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

    private Path writeAiVectorPipelineConfig(
            String name,
            Path data,
            Path stateDir,
            String input,
            String output) throws Exception {
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
                "    fields: [id, content]",
                "  - type: mock-embedding",
                "    input: " + input,
                "    output: " + output,
                "    dimensions: 4",
                "sink:",
                "  type: mock-vector"));
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
