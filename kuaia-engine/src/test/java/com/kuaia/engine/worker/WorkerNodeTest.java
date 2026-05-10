package com.kuaia.engine.worker;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;

class WorkerNodeTest {
    @Test
    void workerNodeDoesNotWriteDirectlyToConsole() throws Exception {
        String source = new String(
                Files.readAllBytes(repoRoot().resolve("kuaia-engine/src/main/java/com/kuaia/engine/worker/WorkerNode.java")),
                StandardCharsets.UTF_8);

        assertFalse(source.contains("System.out.println"), source);
        assertFalse(source.contains("System.err.println"), source);
        assertFalse(source.contains("printStackTrace()"), source);
    }

    private Path repoRoot() {
        Path cwd = Paths.get("").toAbsolutePath();
        if (Files.exists(cwd.resolve("pom.xml")) && Files.exists(cwd.resolve("kuaia-engine"))) {
            return cwd;
        }
        return cwd.getParent();
    }
}
