package com.kuaia.engine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KuaiaCliTest {
    @TempDir
    Path tempDir;

    @Test
    void helpPrintsAvailableCommands() throws Exception {
        CliResult result = run("help");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("Usage: kuaia <command>"));
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

    private CliResult run(String... args) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8.name());
        int exitCode = KuaiaCli.run(args, out);
        out.flush();
        return new CliResult(exitCode, bytes.toString(StandardCharsets.UTF_8.name()));
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
