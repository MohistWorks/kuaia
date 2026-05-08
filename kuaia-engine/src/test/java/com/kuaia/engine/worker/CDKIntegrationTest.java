package com.kuaia.engine.worker;

import com.kuaia.common.api.Collector;
import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.api.SourceReader;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.engine.worker.transform.EmbeddingTransform;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CDKIntegrationTest {
    @Test
    public void testFakeConnectorFlow() throws Exception {
        List<Long> results = new ArrayList<>();

        // 1. Define Fake Source
        SourceReader source = new SourceReader() {
            @Override public void open() {}
            @Override public void close() {}
            @Override public void pollNext(Collector collector) {
                for (long i = 0; i < 10; i++) {
                    BinaryRow row = new BinaryRow(1);
                    row.setLong(0, i);
                    collector.collect(row);
                }
            }
        };

        // 2. Define Fake Sink
        SinkWriter sink = new SinkWriter() {
            @Override public void open() {}
            @Override public void close() {}
            @Override public void write(BinaryRow row) {
                results.add(row.getLong(0));
            }
        };

        // 3. Connect them
        source.pollNext(row -> {
            try {
                sink.write(row);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // 4. Verify
        assertEquals(10, results.size());
        for (int i = 0; i < 10; i++) {
            assertEquals((long) i, (long) results.get(i));
        }
    }

    @Test
    public void embeddingTransformWritesVectorToDedicatedField() throws Exception {
        BinaryRow row = new BinaryRow(3);
        row.setLong(0, 1L);
        row.setString(1, "hello");

        EmbeddingTransform transform = new EmbeddingTransform(1, 2, 1);
        List<BinaryRow> emitted = new ArrayList<>();
        transform.process(row, emitted::addAll);

        assertEquals("hello", emitted.get(0).getString(1));
        assertEquals(4, emitted.get(0).getVector(2).length);
    }
}
