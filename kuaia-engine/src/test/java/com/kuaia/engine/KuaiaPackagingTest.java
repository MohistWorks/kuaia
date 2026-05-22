package com.kuaia.engine;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KuaiaPackagingTest {
    @Test
    void binKuaiaPrintsHelp() throws Exception {
        Path root = repoRoot();
        deletePackagedCliJars(root);
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
        assertTrue(output.contains("validate -f PIPELINE"), output);
        assertTrue(output.contains("benchmark"), output);
    }

    @Test
    void packagingFilesExposeDocumentedWorkflows() throws Exception {
        Path root = repoRoot();
        Path script = root.resolve("bin/kuaia");

        assertTrue(Files.exists(script), "bin/kuaia should exist");
        assertTrue(Files.isExecutable(script), "bin/kuaia should be executable");
        assertTrue(read(script).contains("com.kuaia.engine.KuaiaCli"));
        assertTrue(read(script).contains("PROJECT_VERSION="));
        assertTrue(read(script).contains("kuaia-engine/target/kuaia-engine-$PROJECT_VERSION-cli.jar"));
        assertTrue(read(script).contains("exec java -jar"));
        assertTrue(read(script).contains("-N -DskipTests install"));
        assertTrue(read(script).contains("-pl kuaia-common,kuaia-connectors -DskipTests install"));

        assertTrue(read(root.resolve("Makefile")).contains("run-vector"));
        assertTrue(read(root.resolve("Makefile")).contains("benchmark"));
        assertTrue(read(root.resolve("Makefile")).contains("public-mvp-smoke"));
        assertTrue(read(root.resolve("Makefile")).contains("clean-state"));
        assertTrue(Files.exists(root.resolve("scripts/public-mvp-smoke.sh")), "public MVP smoke script should exist");
        assertTrue(Files.isExecutable(root.resolve("scripts/public-mvp-smoke.sh")),
                "public MVP smoke script should be executable");
        String dockerfile = read(root.resolve("Dockerfile"));
        assertTrue(dockerfile.contains("AS build"), dockerfile);
        assertTrue(dockerfile.contains("VERSION=$(sed -n"), dockerfile);
        assertTrue(dockerfile.contains("cp \"kuaia-engine/target/kuaia-engine-${VERSION}-cli.jar\" /workspace/kuaia.jar"), dockerfile);
        assertTrue(dockerfile.contains("COPY --from=build /workspace/kuaia.jar /opt/kuaia/kuaia.jar"), dockerfile);
        assertTrue(dockerfile.contains("ENTRYPOINT [\"java\", \"-jar\", \"/opt/kuaia/kuaia.jar\"]"), dockerfile);
        String compose = read(root.resolve("docker-compose.yml"));
        assertTrue(compose.contains("examples/local-file-to-file.yaml"), compose);
        assertTrue(compose.contains("/opt/kuaia/.kuaia"), compose);
        assertTrue(read(root.resolve(".dockerignore")).contains("dev/"));
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
        assertTrue(read(root.resolve("README.md")).contains("bin/kuaia benchmark"));
        assertTrue(read(root.resolve("README.md")).contains("--batch-sizes 16,64,256"));
        assertTrue(read(root.resolve("README.md")).contains("--format csv"));
        assertTrue(read(root.resolve("README.md")).contains("target/kuaia-benchmark/local-pipeline-batch.json"));
        assertTrue(read(root.resolve("README.md")).contains("mvn -q package"));
        assertTrue(read(root.resolve("README.md")).contains("kuaia-engine/target/kuaia-engine-${VERSION}-cli.jar"));
        assertTrue(read(root.resolve("README.md")).contains("docker compose up --build"));
        assertTrue(read(root.resolve("README.md")).contains("make release-gate"));
        assertTrue(read(root.resolve("README.md")).contains("make e2e"));
        assertTrue(read(root.resolve("README.md")).contains("make e2e CASE=mysql-qdrant"));
        assertTrue(read(root.resolve("README.md")).contains("make e2e CASE=document-directory-qdrant"));
        assertTrue(read(root.resolve("README.md")).contains("make e2e CASE=s3-qdrant"));
        assertTrue(read(root.resolve("README.md")).contains("make e2e CASE=file-openai-compatible-vector"));
        assertTrue(read(root.resolve("README.md")).contains("make e2e CASE=file-milvus"));
        assertTrue(read(root.resolve("README.md")).contains("make e2e CASE=postgres-pgvector"));
        assertTrue(read(root.resolve("README.md")).contains("DuckDB"));
        assertTrue(read(root.resolve("README.md")).contains("S3-compatible object storage"));
        assertTrue(read(root.resolve("README.md")).contains("document directory"));
        assertTrue(read(root.resolve("README.md")).contains("pgvector"));
        assertTrue(read(root.resolve("README.md")).contains("milvus"));
        assertTrue(read(root.resolve("README.md")).contains(".kuaia/output/local-file-to-file.csv"));
        assertTrue(read(root.resolve("README.md")).contains("SECURITY.md"));
        assertTrue(Files.exists(root.resolve("SECURITY.md")), "SECURITY.md should exist");
        assertTrue(read(root.resolve("CONTRIBUTING.md")).contains("SECURITY.md"));
        assertTrue(Files.exists(root.resolve("docs/examples.md")), "docs/examples.md should exist");
        assertTrue(read(root.resolve("docs/examples.md")).contains("local-file-to-openai-compatible-vector.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("local-file-to-qdrant.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("docker-compose.qdrant.yml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("postgres-to-qdrant.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("postgres-to-pgvector.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("local-file-to-milvus.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("docker-compose.postgres.yml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("mysql-to-qdrant.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("docker-compose.mysql.yml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("duckdb-csv-to-qdrant.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("document-directory-to-qdrant.yaml"));
        assertTrue(read(root.resolve("docs/examples.md")).contains("s3-docs-to-qdrant.yaml"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("sink.type: qdrant"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("sink.type: pgvector"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("sink.type: milvus"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("source.type: postgres"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("source.type: mysql"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("source.type: duckdb"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("source.type: document-directory"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("source.type: s3"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("bin/kuaia benchmark"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("--max-rows-per-split"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("--batch-sizes 16,64,256"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("--format csv"));
        assertTrue(read(root.resolve("docs/pipeline-yaml.md")).contains("docs/examples.md"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("## 0.2.0 release-ready"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("## 0.2.x Roadmap"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("`0.2.1` shipped scope"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("`0.2.2` shipped scope"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("`0.2.3` final `0.2.x` shipped scope"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("the `0.2.2`"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("the final planned `0.2.x` release"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("The `0.2.1` scope shipped"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("DuckDB batch source"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("document-directory source"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("S3-compatible object-storage source"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("pgvector sink"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("Milvus vector sink"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("OpenAI-compatible embedding provider e2e"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("Connector contract tests"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("at least five public changes"));
        assertFalse(read(root.resolve("docs/roadmap.md")).contains("`0.2.4`"));
        assertFalse(read(root.resolve("docs/roadmap.md")).contains("`0.2.5`"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("Connector e2e gate"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("make release-gate"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("Connector-ready runtime"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("MySQL batch source"));
        assertTrue(read(root.resolve("docs/roadmap.md")).contains("0.1.3 shipped"));
        assertTrue(read(root.resolve("docs/README.md")).contains("product-scope.md"));
        assertTrue(read(root.resolve("docs/README.md")).contains("pipeline-yaml.md"));
        assertTrue(read(root.resolve("docs/README.md")).contains("connector-development.md"));
        assertTrue(read(root.resolve("docs/README.md")).contains("release-checklist.md"));
        assertTrue(read(root.resolve("docs/README.md")).contains("../CHANGELOG.md"));
        assertTrue(Files.exists(root.resolve("CHANGELOG.md")), "CHANGELOG.md should exist");
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("## Unreleased"));
        assertFalse(read(root.resolve("CHANGELOG.md")).contains("0.2.3-SNAPSHOT"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("## 0.2.3 - 2026-05-19"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("final `0.2.x` release"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("at least five public changes"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("sink.type: pgvector"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("sink.type: milvus"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("connector contract tests"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("## 0.2.2 - 2026-05-18"));
        assertFalse(read(root.resolve("CHANGELOG.md")).contains("0.2.2-SNAPSHOT"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("## 0.2.1 - 2026-05-18"));
        assertFalse(read(root.resolve("CHANGELOG.md")).contains("0.2.1-SNAPSHOT"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("connector e2e gate"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("make release-gate"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("single-case connector e2e"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("DuckDB batch source"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("document-directory source"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("## 0.2.1 - 2026-05-18"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("## 0.2.0 - 2026-05-17"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("## 0.1.3 - 2026-05-15"));
        assertTrue(read(root.resolve("CHANGELOG.md")).contains("## 0.1.0 - 2026-05-11"));
        assertTrue(read(root.resolve("docs/release-checklist.md")).contains("CHANGELOG.md"));
        assertTrue(read(root.resolve("docs/release-checklist.md")).contains("make release-gate"));
        assertTrue(read(root.resolve("docs/release-checklist.md")).contains("make e2e"));
        assertTrue(read(root.resolve("docs/release-checklist.md")).contains("at least five public changes"));
        assertTrue(read(root.resolve("docs/release-checklist.md")).contains("requires GitHub Actions `CI` to pass"));
        assertTrue(Files.exists(root.resolve("scripts/connector-e2e-smoke.sh")),
                "scripts/connector-e2e-smoke.sh should exist");
        assertTrue(Files.exists(root.resolve("scripts/release-gate.sh")),
                "scripts/release-gate.sh should exist");
        assertTrue(read(root.resolve("scripts/release-gate.sh")).contains("make e2e CASE=all"));
        String e2eScript = read(root.resolve("scripts/connector-e2e-smoke.sh"));
        assertTrue(e2eScript.contains("Available e2e cases"), e2eScript);
        assertTrue(e2eScript.contains("file-qdrant"), e2eScript);
        assertTrue(e2eScript.contains("document-directory-qdrant"), e2eScript);
        assertTrue(e2eScript.contains("s3-qdrant"), e2eScript);
        assertTrue(e2eScript.contains("file-openai-compatible-vector"), e2eScript);
        assertTrue(e2eScript.contains("file-milvus"), e2eScript);
        assertTrue(e2eScript.contains("postgres-qdrant"), e2eScript);
        assertTrue(e2eScript.contains("postgres-pgvector"), e2eScript);
        assertTrue(e2eScript.contains("mysql-qdrant"), e2eScript);
        assertTrue(e2eScript.contains("--list"), e2eScript);
        assertTrue(e2eScript.contains("--help"), e2eScript);
        assertTrue(read(root.resolve("Makefile")).contains("CASE ?= all"));
        assertTrue(read(root.resolve("Makefile")).contains("./scripts/connector-e2e-smoke.sh $(CASE)"));
        assertTrue(read(root.resolve("Makefile")).contains("e2e:"));
        assertTrue(read(root.resolve("Makefile")).contains("release-gate:"));
        assertTrue(Files.exists(root.resolve("docs/connector-development.md")),
                "docs/connector-development.md should exist");
        assertTrue(read(root.resolve("docs/connector-development.md")).contains("BuiltInSourceContractTest"));
        assertTrue(read(root.resolve("docs/connector-development.md")).contains("BuiltInSinkContractTest"));
        assertTrue(Files.exists(root.resolve("docs/release-checklist.md")),
                "docs/release-checklist.md should exist");
        assertTrue(read(root.resolve("docs/product-scope.md")).contains("connector-development.md"));
        assertTrue(read(root.resolve("CONTRIBUTING.md")).contains("make public-mvp-smoke"));
        assertTrue(Files.exists(root.resolve("examples/local-file-to-qdrant.yaml")), "Qdrant example should exist");
        assertTrue(Files.exists(root.resolve("docker-compose.qdrant.yml")), "Qdrant compose file should exist");
        assertTrue(Files.exists(root.resolve("examples/postgres-to-qdrant.yaml")), "Postgres to Qdrant example should exist");
        assertTrue(Files.exists(root.resolve("examples/postgres-to-pgvector.yaml")),
                "Postgres to pgvector example should exist");
        assertTrue(Files.exists(root.resolve("examples/local-file-to-milvus.yaml")),
                "Local file to Milvus example should exist");
        assertTrue(Files.exists(root.resolve("docker-compose.postgres.yml")), "Postgres compose file should exist");
        assertTrue(Files.exists(root.resolve("examples/postgres/init/01-documents.sql")), "Postgres init SQL should exist");
        assertTrue(read(root.resolve("examples/postgres/init/01-documents.sql")).contains("create extension if not exists vector"));
        assertTrue(read(root.resolve("docker-compose.postgres.yml")).contains("pgvector/pgvector"));
        assertTrue(Files.exists(root.resolve("examples/mysql-to-qdrant.yaml")), "MySQL to Qdrant example should exist");
        assertTrue(Files.exists(root.resolve("docker-compose.mysql.yml")), "MySQL compose file should exist");
        assertTrue(Files.exists(root.resolve("examples/mysql/init/01-documents.sql")), "MySQL init SQL should exist");
        assertTrue(Files.exists(root.resolve("examples/duckdb-csv-to-qdrant.yaml")),
                "DuckDB to Qdrant example should exist");
        assertTrue(Files.exists(root.resolve("examples/document-directory-to-qdrant.yaml")),
                "Document directory to Qdrant example should exist");
        assertTrue(Files.exists(root.resolve("examples/s3-docs-to-qdrant.yaml")),
                "S3 to Qdrant example should exist");
        assertTrue(Files.exists(root.resolve("examples/data/docs/intro.md")),
                "Document directory example data should exist");
        assertTrue(read(root.resolve("scripts/connector-e2e-smoke.sh")).contains("duckdb-qdrant"));
        assertTrue(read(root.resolve("scripts/connector-e2e-smoke.sh")).contains("document-directory-qdrant"));
        assertTrue(read(root.resolve("scripts/connector-e2e-smoke.sh")).contains("s3-qdrant"));
        assertTrue(read(root.resolve("scripts/connector-e2e-smoke.sh")).contains("file-openai-compatible-vector"));
        assertTrue(read(root.resolve("scripts/connector-e2e-smoke.sh")).contains("file-milvus"));
        assertTrue(read(root.resolve("scripts/connector-e2e-smoke.sh")).contains("postgres-pgvector"));
        assertTrue(read(root.resolve("scripts/release-gate.sh"))
                .contains("bin/kuaia validate -f examples/duckdb-csv-to-qdrant.yaml"));
        assertTrue(read(root.resolve("scripts/release-gate.sh"))
                .contains("bin/kuaia validate -f examples/document-directory-to-qdrant.yaml"));
        assertTrue(read(root.resolve("scripts/release-gate.sh"))
                .contains("bin/kuaia validate -f examples/s3-docs-to-qdrant.yaml"));
        assertTrue(read(root.resolve("scripts/release-gate.sh"))
                .contains("bin/kuaia validate -f examples/local-file-to-openai-compatible-vector.yaml"));
        assertTrue(read(root.resolve("scripts/release-gate.sh"))
                .contains("bin/kuaia validate -f examples/local-file-to-milvus.yaml"));
        assertTrue(read(root.resolve("scripts/release-gate.sh"))
                .contains("bin/kuaia validate -f examples/postgres-to-pgvector.yaml"));

        String enginePom = read(root.resolve("kuaia-engine/pom.xml"));
        assertTrue(read(root.resolve("pom.xml")).contains("<version>0.3.0-SNAPSHOT</version>"));
        assertTrue(read(root.resolve("kuaia-common/pom.xml")).contains("<version>0.3.0-SNAPSHOT</version>"));
        assertTrue(enginePom.contains("<version>0.3.0-SNAPSHOT</version>"), enginePom);
        assertTrue(enginePom.contains("maven-shade-plugin"), enginePom);
        assertTrue(enginePom.contains("mysql-connector-j"), enginePom);
        assertTrue(enginePom.contains("postgresql"), enginePom);
        assertTrue(enginePom.contains("duckdb_jdbc"), enginePom);
        assertTrue(enginePom.contains("<artifactId>s3</artifactId>"), enginePom);
        assertTrue(enginePom.contains("com.kuaia.engine.KuaiaCli"), enginePom);
        assertTrue(enginePom.contains("<createDependencyReducedPom>false</createDependencyReducedPom>"), enginePom);
        assertTrue(enginePom.contains("<shadedArtifactAttached>true</shadedArtifactAttached>"), enginePom);
        assertTrue(enginePom.contains("<shadedClassifierName>cli</shadedClassifierName>"), enginePom);
        assertTrue(enginePom.contains("ServicesResourceTransformer"), enginePom);
    }

    @Test
    void ciWorkflowCoversOpenSourceSmokePaths() throws Exception {
        Path root = repoRoot();
        String workflow = read(root.resolve(".github/workflows/ci.yml"));

        assertTrue(workflow.contains("workflow_dispatch:"), workflow);
        assertTrue(workflow.contains("make release-gate"), workflow);
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

    private void deletePackagedCliJars(Path root) throws Exception {
        Path target = root.resolve("kuaia-engine/target");
        if (!Files.isDirectory(target)) {
            return;
        }
        try (DirectoryStream<Path> jars = Files.newDirectoryStream(target, "kuaia-engine-*-cli.jar")) {
            for (Path jar : jars) {
                Files.deleteIfExists(jar);
            }
        }
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
