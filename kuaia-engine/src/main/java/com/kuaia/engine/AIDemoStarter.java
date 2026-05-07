package com.kuaia.engine;

import com.kuaia.engine.worker.connector.MockVectorSink;
import com.kuaia.engine.worker.connector.FakeSource;
import com.kuaia.engine.worker.transform.EmbeddingTransform;

public class AIDemoStarter {
    public static void main(String[] args) throws Exception {
        FakeSource source = new FakeSource();
        EmbeddingTransform transformer = new EmbeddingTransform();
        MockVectorSink sink = new MockVectorSink(source.getRowType());
        
        source.open();
        sink.open();
        
        System.out.println("Starting AI Vector Demo Pipeline...");
        // Run 10 iterations (2 batches of 5)
        for (int i = 0; i < 10; i++) {
            source.pollNext(row -> {
                transformer.process(row, batch -> {
                    for (com.kuaia.common.data.BinaryRow bRow : batch) {
                        try {
                            sink.write(bRow);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            });
        }
        
        source.close();
        sink.close();
        System.out.println("AI Vector Demo Finished.");
    }
}
