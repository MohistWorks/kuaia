package com.kuaia.engine;

import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineConfigLoader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

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
                "examples/local-file-to-file.yaml",
                "examples/local-file-skip-bad-records.yaml");

        Path fileSinkOutput = repoRoot().resolve(".kuaia/output/local-file-to-file.csv");
        Files.deleteIfExists(fileSinkOutput);

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
    }

    @Test
    void externalServiceExamplesIncludeTuningOptions() throws Exception {
        Path fileToQdrantPath = repoRoot().resolve("examples/local-file-to-qdrant.yaml");
        Path chunkToQdrantPath = repoRoot().resolve("examples/local-jsonl-chunk-to-qdrant.yaml");
        Path postgresToQdrantPath = repoRoot().resolve("examples/postgres-to-qdrant.yaml");
        PipelineConfig fileToQdrant = new PipelineConfigLoader().load(fileToQdrantPath);
        PipelineConfig chunkToQdrant = new PipelineConfigLoader().load(chunkToQdrantPath);
        PipelineConfig postgresToQdrant = new PipelineConfigLoader().load(postgresToQdrantPath);

        assertTrue(read(fileToQdrantPath).contains("timeoutMs: 30000"));
        assertTrue(read(chunkToQdrantPath).contains("chunkIndexField: chunk_index"));
        assertTrue(read(chunkToQdrantPath).contains("chunkIdMultiplier: 1000000"));
        assertTrue(read(chunkToQdrantPath).contains("dropInput: true"));
        assertTrue(read(chunkToQdrantPath).contains("includeOffsets: true"));
        assertTrue(read(postgresToQdrantPath).contains("fetchSize: 1000"));
        assertTrue(read(postgresToQdrantPath).contains("timeoutMs: 30000"));
        assertEquals(30000, fileToQdrant.getSink().getTimeoutMs());
        assertEquals("chunk_index", chunkToQdrant.getSink().getChunkIndexField());
        assertEquals(1_000_000L, chunkToQdrant.getSink().getChunkIdMultiplier());
        assertEquals(1000, postgresToQdrant.getSource().getFetchSize());
        assertEquals(30000, postgresToQdrant.getSink().getTimeoutMs());
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
