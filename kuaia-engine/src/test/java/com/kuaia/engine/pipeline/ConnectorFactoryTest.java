package com.kuaia.engine.pipeline;

import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConnectorFactoryTest {
    @Test
    void legacyDocumentDirectoryTypeFailsWithMigrationHint() {
        // A coordinator replaying a persisted pre-upgrade task bypasses the YAML loader, so the
        // factory itself must surface the same migration hint as PipelineConfigLoader.
        PipelineConfig config = new PipelineConfig(
                "legacy-document-directory",
                new PipelineConfig.SourceConfig("document-directory", "data/docs", null),
                new PipelineConfig.SinkConfig("console"),
                new PipelineConfig.CheckpointConfig(null));
        ConnectorFactory factory = new ConnectorFactory(SinkFactoryRegistry.defaultRegistry());

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> factory.createSource(config));

        assertEquals(
                "source.type document-directory has been replaced by source.type: file with format: document",
                error.getMessage());
    }

    @Test
    void legacyS3TypeFailsWithMigrationHint() {
        // A coordinator replaying a persisted pre-upgrade task bypasses the YAML loader, so the
        // factory itself must surface the same migration hint as PipelineConfigLoader.
        PipelineConfig config = new PipelineConfig(
                "legacy-s3",
                new PipelineConfig.SourceConfig("s3", "docs", null),
                new PipelineConfig.SinkConfig("console"),
                new PipelineConfig.CheckpointConfig(null));
        ConnectorFactory factory = new ConnectorFactory(SinkFactoryRegistry.defaultRegistry());

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> factory.createSource(config));

        assertEquals(
                "source.type s3 has been replaced by source.type: file with an s3:// path",
                error.getMessage());
    }
}
