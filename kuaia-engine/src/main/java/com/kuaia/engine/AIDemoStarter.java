package com.kuaia.engine;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.worker.connector.MockVectorSink;
import com.kuaia.engine.worker.transform.EmbeddingTransform;

public class AIDemoStarter {
    public static void main(String[] args) throws Exception {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "content", "embedding"},
                new DataType[]{DataType.LONG, DataType.STRING, DataType.VECTOR}
        );
        EmbeddingTransform transformer = new EmbeddingTransform();
        MockVectorSink sink = new MockVectorSink(rowType);

        sink.open();

        System.out.println("Starting AI Vector Demo Pipeline...");
        // Run 10 iterations (2 batches of 5)
        for (int i = 0; i < 10; i++) {
            BinaryRow row = new BinaryRow(3);
            row.setLong(0, i + 1L);
            row.setString(1, "Document-" + (i + 1));
            transformer.process(row, batch -> {
                for (BinaryRow bRow : batch) {
                    try {
                        sink.write(bRow);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        sink.close();
        System.out.println("AI Vector Demo Finished.");
    }
}
