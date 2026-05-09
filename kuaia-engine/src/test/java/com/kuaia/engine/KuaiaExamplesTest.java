package com.kuaia.engine;

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
                "examples/local-file-to-vector.yaml");

        for (String example : examples) {
            CliResult result = run("run", "-f", repoRoot().resolve(example).toString());

            assertEquals(0, result.exitCode, example + "\n" + result.output);
            assertTrue(result.output.contains("Starting pipeline:"), result.output);
            assertTrue(result.output.contains("Pipeline Finished."), result.output);
        }
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

    private static class CliResult {
        private final int exitCode;
        private final String output;

        private CliResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
