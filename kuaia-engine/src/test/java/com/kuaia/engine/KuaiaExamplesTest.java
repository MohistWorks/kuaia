package com.kuaia.engine;

import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineConfigLoader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KuaiaExamplesTest {
    @Test
    void publicExamplesRunSuccessfully() throws Exception {
        List<String> examples = Arrays.asList(
                "examples/local-file-to-console.yaml",
                "examples/local-file-transform-to-console.yaml",
                "examples/local-file-to-vector.yaml",
                "examples/local-jsonl-to-vector.yaml",
                "examples/local-jsonl-chunk-to-vector.yaml",
                "examples/local-faq-jsonl-to-vector.yaml",
                "examples/local-file-to-file.yaml",
                "examples/local-quoted-csv-to-file.yaml",
                "examples/local-jsonl-to-file.yaml",
                "examples/local-file-skip-bad-records.yaml");

        Path fileSinkOutput = repoRoot().resolve(".kuaia/output/local-file-to-file.csv");
        Path quotedCsvSinkOutput = repoRoot().resolve(".kuaia/output/local-quoted-csv-to-file.csv");
        Path jsonlSinkOutput = repoRoot().resolve(".kuaia/output/local-jsonl-to-file.jsonl");
        for (String stateDir : Arrays.asList(
                "local-file-to-console",
                "local-file-transform-to-console",
                "local-file-to-vector",
                "local-jsonl-to-vector",
                "local-jsonl-chunk-to-vector",
                "local-faq-jsonl-to-vector",
                "local-file-to-file",
                "local-quoted-csv-to-file",
                "local-jsonl-to-file",
                "local-file-skip-bad-records")) {
            deleteExampleState(stateDir);
        }
        Files.deleteIfExists(fileSinkOutput);
        Files.deleteIfExists(quotedCsvSinkOutput);
        Files.deleteIfExists(jsonlSinkOutput);

        for (String example : examples) {
            CliResult result = run("run", "-f", repoRoot().resolve(example).toString());

            assertEquals(0, result.exitCode, example + "\n" + result.output);
            assertTrue(result.output.contains("Starting pipeline:"), result.output);
            assertTrue(result.output.contains("Pipeline Finished."), result.output);
            assertTrue(result.output.contains("Run Summary:"), result.output);
        }

        assertEquals(
                Arrays.asList("id,name", "1,Alice", "2,Bob"),
                Files.readAllLines(fileSinkOutput, StandardCharsets.UTF_8));
        String nl = System.lineSeparator();
        assertEquals(
                "id,content" + nl
                        + "1,\"Alpha, \"\"Beta\"\"\"" + nl
                        + "2,\"Line one\nLine two\"" + nl,
                new String(Files.readAllBytes(quotedCsvSinkOutput), StandardCharsets.UTF_8));
        assertEquals(
                Arrays.asList(
                        "{\"id\":1,\"content\":\"alfa\"}"),
                Files.readAllLines(jsonlSinkOutput, StandardCharsets.UTF_8));
    }

    @Test
    void clusterDemoExampleIsValid() throws Exception {
        PipelineConfig demo = new PipelineConfigLoader()
                .load(repoRoot().resolve("examples/cluster-demo/pipeline.yaml"));

        assertEquals("cluster-demo", demo.getName());
        assertEquals("file", demo.getSource().getType());
        assertEquals("file", demo.getSink().getType());
    }

    @Test
    void externalServiceExamplesIncludeTuningOptions() throws Exception {
        Path fileToQdrantPath = repoRoot().resolve("examples/local-file-to-qdrant.yaml");
        Path chunkToQdrantPath = repoRoot().resolve("examples/local-jsonl-chunk-to-qdrant.yaml");
        Path postgresToQdrantPath = repoRoot().resolve("examples/postgres-to-qdrant.yaml");
        Path mysqlToQdrantPath = repoRoot().resolve("examples/mysql-to-qdrant.yaml");
        Path s3ToQdrantPath = repoRoot().resolve("examples/s3-docs-to-qdrant.yaml");
        Path fileToMilvusPath = repoRoot().resolve("examples/local-file-to-milvus.yaml");
        PipelineConfig fileToQdrant = new PipelineConfigLoader().load(fileToQdrantPath);
        PipelineConfig chunkToQdrant = new PipelineConfigLoader().load(chunkToQdrantPath);
        PipelineConfig postgresToQdrant = new PipelineConfigLoader().load(postgresToQdrantPath);
        PipelineConfig mysqlToQdrant = new PipelineConfigLoader().load(mysqlToQdrantPath);
        PipelineConfig s3ToQdrant = new PipelineConfigLoader().load(s3ToQdrantPath);
        PipelineConfig fileToMilvus = new PipelineConfigLoader().load(fileToMilvusPath);

        assertTrue(read(fileToQdrantPath).contains("timeoutMs: 30000"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-vector.yaml")).contains("type: trim"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-vector.yaml")).contains("op: not-empty"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("type: lowercase"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("type: replace"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("target: ph"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("replacement: f"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("op: min-length"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("minLength: 4"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("op: contains"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("value: a"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("op: starts-with"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("op: equals"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("value: alfa"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("field: id"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("op: less-than"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-to-file.yaml")).contains("value: 2"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-chunk-to-vector.yaml")).contains("type: trim"));
        assertTrue(read(repoRoot().resolve("examples/local-jsonl-chunk-to-vector.yaml")).contains("op: not-empty"));
        assertTrue(read(repoRoot().resolve("examples/local-faq-jsonl-to-vector.yaml")).contains("fields: [id, question, answer]"));
        assertTrue(read(repoRoot().resolve("examples/local-faq-jsonl-to-vector.yaml")).contains("input: answer"));
        assertTrue(read(repoRoot().resolve("examples/local-faq-jsonl-to-vector.yaml")).contains("batchSize: 32"));
        assertTrue(read(chunkToQdrantPath).contains("type: trim"));
        assertTrue(read(chunkToQdrantPath).contains("op: not-empty"));
        assertTrue(read(chunkToQdrantPath).contains("chunkIndexField: chunk_index"));
        assertTrue(read(chunkToQdrantPath).contains("chunkIdMultiplier: 1000000"));
        assertTrue(read(chunkToQdrantPath).contains("payloadFields: [id, chunk, chunk_index, chunk_start, chunk_end]"));
        assertTrue(read(chunkToQdrantPath).contains("dropInput: true"));
        assertTrue(read(chunkToQdrantPath).contains("includeOffsets: true"));
        assertTrue(read(postgresToQdrantPath).contains("fetchSize: 1000"));
        assertTrue(read(postgresToQdrantPath).contains("timeoutMs: 30000"));
        assertTrue(read(mysqlToQdrantPath).contains("fetchSize: 1000"));
        assertTrue(read(mysqlToQdrantPath).contains("timeoutMs: 30000"));
        assertTrue(read(s3ToQdrantPath).contains("pathStyleAccess: true"));
        assertTrue(read(fileToMilvusPath).contains("apiKeyEnv: KUAIA_MILVUS_TOKEN"));
        assertEquals(30000, fileToQdrant.getSink().getTimeoutMs());
        assertEquals(Arrays.asList("id", "content"), fileToQdrant.getSink().getPayloadFields());
        assertEquals("chunk_index", chunkToQdrant.getSink().getChunkIndexField());
        assertEquals(1_000_000L, chunkToQdrant.getSink().getChunkIdMultiplier());
        assertEquals(
                Arrays.asList("id", "chunk", "chunk_index", "chunk_start", "chunk_end"),
                chunkToQdrant.getSink().getPayloadFields());
        assertEquals(1000, postgresToQdrant.getSource().getFetchSize());
        assertEquals(30000, postgresToQdrant.getSink().getTimeoutMs());
        assertEquals(Arrays.asList("id", "content"), postgresToQdrant.getSink().getPayloadFields());
        assertEquals("s3", s3ToQdrant.getSource().getType());
        assertEquals("kuaia-docs", s3ToQdrant.getSource().getBucket());
        assertEquals("docs/", s3ToQdrant.getSource().getPrefix());
        assertEquals(Arrays.asList("id", "key", "content"), s3ToQdrant.getSink().getPayloadFields());
        assertEquals("mysql", mysqlToQdrant.getSource().getType());
        assertEquals(1000, mysqlToQdrant.getSource().getFetchSize());
        assertEquals(30000, mysqlToQdrant.getSink().getTimeoutMs());
        assertEquals(Arrays.asList("id", "content"), mysqlToQdrant.getSink().getPayloadFields());
        assertEquals("milvus", fileToMilvus.getSink().getType());
        assertEquals("kuaia_docs", fileToMilvus.getSink().getCollection());
        assertEquals(Arrays.asList("content"), fileToMilvus.getSink().getPayloadFields());
    }

    private CliResult run(String... args) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8.name());
        int exitCode = KuaiaCli.run(args, out);
        out.flush();
        return new CliResult(exitCode, bytes.toString(StandardCharsets.UTF_8.name()));
    }

    private Path repoRoot() {
        Path cwd = Paths.get("").toAbsolutePath();
        if (Files.exists(cwd.resolve("pom.xml")) && Files.exists(cwd.resolve("kuaia-engine"))) {
            return cwd;
        }
        return cwd.getParent();
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(file -> {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private void deleteExampleState(String stateDir) throws IOException {
        deleteRecursively(Paths.get(".kuaia/state").resolve(stateDir).toAbsolutePath().normalize());
        deleteRecursively(repoRoot().resolve(".kuaia/state").resolve(stateDir));
    }

    private String read(Path path) throws Exception {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
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
