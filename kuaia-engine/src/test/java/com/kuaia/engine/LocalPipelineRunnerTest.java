package com.kuaia.engine;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineConfigLoader;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import com.kuaia.engine.worker.connector.VectorSinkFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LocalPipelineRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void writesVectorPipelineRowsInConfiguredBatches() throws Exception {
        Path data = tempDir.resolve("documents.csv");
        Files.write(data, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta",
                "3,Gamma").getBytes(StandardCharsets.UTF_8));
        Path configPath = tempDir.resolve("batched-vector.yaml");
        Files.write(configPath, String.join("\n",
                "name: batched-vector",
                "source:",
                "  type: file",
                "  path: " + data,
                "  format: csv",
                "transforms:",
                "  - type: mock-embedding",
                "    input: content",
                "    output: embedding",
                "    dimensions: 4",
                "    batchSize: 2",
                "sink:",
                "  type: mock-vector").getBytes(StandardCharsets.UTF_8));
        CapturingSink sink = new CapturingSink();
        PipelineConfig config = new PipelineConfigLoader().load(configPath);
        SinkFactoryRegistry registry = new SinkFactoryRegistry(Collections.singletonMap(
                "mock-vector",
                (VectorSinkFactory) (rowType, out, sinkConfig) -> sink));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new LocalPipelineRunner(registry).run(config, new PrintStream(bytes, true, StandardCharsets.UTF_8.name()));

        assertEquals(0, sink.singleWrites);
        assertEquals(2, sink.batchWrites);
        assertEquals(java.util.Arrays.asList(2, 1), sink.batchSizes);
    }

    private static class CapturingSink implements SinkWriter {
        private int singleWrites;
        private int batchWrites;
        private final List<Integer> batchSizes = new ArrayList<>();

        @Override
        public void open() {}

        @Override
        public void write(BinaryRow row) {
            singleWrites++;
        }

        @Override
        public void writeBatch(List<BinaryRow> rows) {
            batchWrites++;
            batchSizes.add(rows.size());
        }

        @Override
        public void close() {}
    }
}
