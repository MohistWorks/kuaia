package com.kuaia.engine;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KuaiaPackagingTest {
    @Test
    void binKuaiaPrintsHelp() throws Exception {
        Path root = repoRoot();
        Process process = new ProcessBuilder(root.resolve("bin/kuaia").toString(), "help")
                .directory(root.toFile())
                .redirectErrorStream(true)
                .start();

        boolean finished = process.waitFor(90, TimeUnit.SECONDS);
        String output = read(process.getInputStream());

        assertTrue(finished, "bin/kuaia help did not finish. Output:\n" + output);
        assertEquals(0, process.exitValue(), output);
        assertTrue(output.contains("Usage: kuaia <command>"), output);
        assertTrue(output.contains("run -f PIPELINE"), output);
    }

    @Test
    void packagingFilesExposeDocumentedWorkflows() throws Exception {
        Path root = repoRoot();
        Path script = root.resolve("bin/kuaia");

        assertTrue(Files.exists(script), "bin/kuaia should exist");
        assertTrue(Files.isExecutable(script), "bin/kuaia should be executable");
        assertTrue(read(script).contains("com.kuaia.engine.KuaiaCli"));
        assertTrue(read(script).contains("-N -DskipTests install"));
        assertTrue(read(script).contains("-pl kuaia-common -DskipTests install"));

        assertTrue(read(root.resolve("Makefile")).contains("run-vector"));
        assertTrue(read(root.resolve("Makefile")).contains("clean-state"));
        assertTrue(read(root.resolve("Dockerfile")).contains("ENTRYPOINT [\"./bin/kuaia\"]"));
        assertTrue(read(root.resolve("docker-compose.yml")).contains("examples/local-file-to-vector.yaml"));
        assertTrue(read(root.resolve(".dockerignore")).contains("dev/"));
        assertTrue(read(root.resolve(".dockerignore")).contains("daily_tasks.json"));
        assertTrue(Files.exists(root.resolve("docs/pipeline-yaml.md")), "docs/pipeline-yaml.md should exist");
        assertTrue(read(root.resolve("README.md")).contains("docs/pipeline-yaml.md"));
    }

    @Test
    void ciWorkflowCoversOpenSourceSmokePaths() throws Exception {
        Path root = repoRoot();
        String workflow = read(root.resolve(".github/workflows/ci.yml"));

        assertTrue(workflow.contains("workflow_dispatch:"), workflow);
        assertTrue(workflow.contains("mvn -q test"), workflow);
        assertTrue(workflow.contains("bin/kuaia help"), workflow);
        assertTrue(workflow.contains("bin/kuaia run -f examples/local-file-to-file.yaml"), workflow);
        assertTrue(workflow.contains("bin/kuaia run -f examples/local-file-skip-bad-records.yaml"), workflow);
        assertTrue(workflow.contains("docker compose config"), workflow);
    }

    private Path repoRoot() {
        Path cwd = Paths.get("").toAbsolutePath();
        if (Files.exists(cwd.resolve("pom.xml")) && Files.exists(cwd.resolve("kuaia-engine"))) {
            return cwd;
        }
        return cwd.getParent();
    }

    private String read(Path path) throws Exception {
        if (Files.isDirectory(path)) {
            throw new IllegalArgumentException("Cannot read directory " + path);
        }
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private String read(InputStream input) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            bytes.write(buffer, 0, read);
        }
        return bytes.toString(StandardCharsets.UTF_8.name());
    }
}
