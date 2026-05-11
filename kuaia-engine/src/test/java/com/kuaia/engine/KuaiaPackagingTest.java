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
        assertTrue(read(script).contains("kuaia-engine/target/kuaia-engine-0.1.0-SNAPSHOT-cli.jar"));
        assertTrue(read(script).contains("exec java -jar"));
        assertTrue(read(script).contains("-N -DskipTests install"));
        assertTrue(read(script).contains("-pl kuaia-common -DskipTests install"));

        assertTrue(read(root.resolve("Makefile")).contains("run-vector"));
        assertTrue(read(root.resolve("Makefile")).contains("public-mvp-smoke"));
        assertTrue(read(root.resolve("Makefile")).contains("clean-state"));
        assertTrue(Files.exists(root.resolve("scripts/public-mvp-smoke.sh")), "public MVP smoke script should exist");
        assertTrue(Files.isExecutable(root.resolve("scripts/public-mvp-smoke.sh")),
                "public MVP smoke script should be executable");
        String dockerfile = read(root.resolve("Dockerfile"));
        assertTrue(dockerfile.contains("AS build"), dockerfile);
        assertTrue(dockerfile.contains("COPY --from=build /workspace/kuaia-engine/target/kuaia-engine-0.1.0-SNAPSHOT-cli.jar /opt/kuaia/kuaia.jar"), dockerfile);
        assertTrue(dockerfile.contains("ENTRYPOINT [\"java\", \"-jar\", \"/opt/kuaia/kuaia.jar\"]"), dockerfile);
        String compose = read(root.resolve("docker-compose.yml"));
        assertTrue(compose.contains("examples/local-file-to-file.yaml"), compose);
        assertTrue(compose.contains("/opt/kuaia/.kuaia"), compose);
        assertTrue(read(root.resolve(".dockerignore")).contains("dev/"));
        assertTrue(read(root.resolve(".dockerignore")).contains("daily_tasks.json"));
        assertTrue(read(root.resolve(".gitignore")).contains("daily_tasks.json"));
        assertTrue(Files.exists(root.resolve(".github/ISSUE_TEMPLATE/bug_report.yml")),
                "bug report issue template should exist");
        assertTrue(Files.exists(root.resolve(".github/ISSUE_TEMPLATE/feature_request.yml")),
                "feature request issue template should exist");
        assertTrue(Files.exists(root.resolve(".github/ISSUE_TEMPLATE/config.yml")),
                "issue template config should exist");
        assertTrue(Files.exists(root.resolve(".github/pull_request_template.md")),
                "pull request template should exist");
        assertTrue(read(root.resolve(".github/pull_request_template.md")).contains("make public-mvp-smoke"));
        assertTrue(read(root.resolve(".github/ISSUE_TEMPLATE/bug_report.yml")).contains("Do not include API keys"));
        assertTrue(Files.exists(root.resolve("docs/README.md")), "docs/README.md should exist");
        assertTrue(Files.exists(root.resolve("docs/pipeline-yaml.md")), "docs/pipeline-yaml.md should exist");
        assertTrue(read(root.resolve("README.md")).contains("docs/README.md"));
        assertTrue(read(root.resolve("README.md")).contains("actions/workflows/ci.yml/badge.svg"));
        assertTrue(read(root.resolve("README.md")).contains("docs/pipeline-yaml.md"));
        assertTrue(read(root.resolve("README.md")).contains("docs/examples.md"));
        assertTrue(read(root.resolve("README.md")).contains("docs/connector-development.md"));
        assertTrue(read(root.resolve("README.md")).contains("docs/release-checklist.md"));
        assertTrue(read(root.resolve("README.md")).contains("CHANGELOG.md"));
        assertTrue(read(root.resolve("README.md")).contains("make public-mvp-smoke"));
        assertTrue(read(root.resolve("README.md")).contains("mvn -q package"));
        assertTrue(read(root.resolve("README.md")).contains("java -jar kuaia-engine/target/kuaia-engine-0.1.0-SNAPSHOT-cli.jar help"));
        assertTrue(read(root.resolve("README.md")).contains("docker compose up --build"));
        assertTrue(read(root.resolve("README.md")).contains(".kuaia/output/local-file-to-file.csv"));
        assertTrue(read(root.resolve("README.md")).contains("SECURITY.md"));
        assertTrue(Files.exists(root.resolve("SECURITY.md")), "SECURITY.md should exist");
        assertTrue(read(root.resolve("CONTRIBUTING.md")).contains("SECURITY.md"));
        assertTrue(Files.exists(root.resolve("docs/examples.md")), "docs/examples.md should exist");
        assertTrue(read(root.resolve("docs/examples.md")).contains("local-file-to-openai-compatible-vector.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("local-file-to-qdrant.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("docker-compose.qdrant.yml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("postgres-to-qdrant.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("docker-compose.postgres.yml"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("sink.type: qdrant"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("source.type: postgres"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("docs/examples.md"));
        assertTrue(read(root.resolve("docs/README.md")).contains("product-scope.md"));
        assertTrue(read(root.resolve("docs/README.md")).contains("pipeline-yaml.md"));
        assertTrue(read(root.resolve("docs/README.md")).contains("connector-development.md"));
        assertTrue(read(root.resolve("docs/README.md")).contains("release-checklist.md"));
        assertTrue(read(root.resolve("docs/README.md")).contains("../CHANGELOG.md"));
        assertTrue(Files.exists(root.resolve("CHANGELOG.md")), "CHANGELOG.md should exist");
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("Unreleased MVP"));
        assertTrue(read(root.resolve("docs/release-checklist.md")).contains("CHANGELOG.md"));
        assertTrue(Files.exists(root.resolve("docs/connector-development.md")),
                "docs/connector-development.md should exist");
        assertTrue(Files.exists(root.resolve("docs/release-checklist.md")),
                "docs/release-checklist.md should exist");
        assertTrue(read(root.resolve("docs/product-scope.md")).contains("connector-development.md"));
        assertTrue(read(root.resolve("CONTRIBUTING.md")).contains("make public-mvp-smoke"));
        assertTrue(Files.exists(root.resolve("examples/local-file-to-qdrant.yaml")), "Qdrant example should exist");
        assertTrue(Files.exists(root.resolve("docker-compose.qdrant.yml")), "Qdrant compose file should exist");
        assertTrue(Files.exists(root.resolve("examples/postgres-to-qdrant.yaml")), "Postgres to Qdrant example should exist");
        assertTrue(Files.exists(root.resolve("docker-compose.postgres.yml")), "Postgres compose file should exist");
        assertTrue(Files.exists(root.resolve("examples/postgres/init/01-documents.sql")), "Postgres init SQL should exist");

        String enginePom = read(root.resolve("kuaia-engine/pom.xml"));
        assertTrue(enginePom.contains("maven-shade-plugin"), enginePom);
        assertTrue(enginePom.contains("com.kuaia.engine.KuaiaCli"), enginePom);
        assertTrue(enginePom.contains("<createDependencyReducedPom>false</createDependencyReducedPom>"), enginePom);
        assertTrue(enginePom.contains("<shadedArtifactAttached>true</shadedArtifactAttached>"), enginePom);
        assertTrue(enginePom.contains("<shadedClassifierName>cli</shadedClassifierName>"), enginePom);
    }

    @Test
    void ciWorkflowCoversOpenSourceSmokePaths() throws Exception {
        Path root = repoRoot();
        String workflow = read(root.resolve(".github/workflows/ci.yml"));

        assertTrue(workflow.contains("workflow_dispatch:"), workflow);
        assertTrue(workflow.contains("mvn -q test"), workflow);
        assertTrue(workflow.contains("mvn -q package"), workflow);
        assertTrue(workflow.contains("make public-mvp-smoke"), workflow);
        assertTrue(workflow.contains("java -jar kuaia-engine/target/kuaia-engine-0.1.0-SNAPSHOT-cli.jar help"), workflow);
        assertTrue(workflow.contains("bin/kuaia help"), workflow);
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
