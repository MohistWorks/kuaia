package com.kuaia.engine.pipeline;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineConfigSerializationTest {
    @Test
    void roundTripsThroughJavaSerialization() throws Exception {
        PipelineConfig config = new PipelineConfig(
                "job-1",
                new PipelineConfig.SourceConfig("file", "/tmp/in.csv", "csv"),
                new PipelineConfig.SinkConfig("file", "/tmp/out.csv", "csv", null),
                new PipelineConfig.CheckpointConfig(null));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(config);
        }
        PipelineConfig restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (PipelineConfig) in.readObject();
        }

        assertEquals("job-1", restored.getName());
        assertEquals("file", restored.getSource().getType());
        assertEquals("/tmp/out.csv", restored.getSink().getPath());
    }

    @Test
    void roundTripsTransformConfigAndErrorPolicy() throws Exception {
        PipelineConfig.TransformConfig transform = new PipelineConfig.TransformConfig(
                "rename", List.of("col_a"), "col_a", "column_a");

        PipelineConfig config = new PipelineConfig(
                "job-2",
                new PipelineConfig.SourceConfig("file", "/tmp/in.csv", "csv"),
                List.of(transform),
                new PipelineConfig.SinkConfig("file", "/tmp/out.csv", "csv", null),
                new PipelineConfig.CheckpointConfig(null));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(config);
        }
        PipelineConfig restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (PipelineConfig) in.readObject();
        }

        assertEquals(1, restored.getTransforms().size());
        assertEquals("rename", restored.getTransforms().get(0).getType());
        assertEquals("column_a", restored.getTransforms().get(0).getTo());
        assertEquals("fail-fast", restored.getErrorPolicy().getMode());
    }
}
