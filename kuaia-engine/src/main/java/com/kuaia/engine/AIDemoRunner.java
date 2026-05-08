package com.kuaia.engine;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.worker.connector.MockVectorSink;
import com.kuaia.engine.worker.transform.EmbeddingTransform;

import java.io.PrintStream;

public class AIDemoRunner {
    public int run(PrintStream out) throws Exception {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "content", "embedding"},
                new DataType[]{DataType.LONG, DataType.STRING, DataType.VECTOR});
        EmbeddingTransform transformer = new EmbeddingTransform();
        MockVectorSink sink = new MockVectorSink(rowType, out);

        sink.open();
        int rows = 0;
        try {
            out.println("Starting AI Vector Demo Pipeline...");
            for (int i = 0; i < 10; i++) {
                BinaryRow row = new BinaryRow(3);
                row.setLong(0, i + 1L);
                row.setString(1, "Document-" + (i + 1));
                transformer.process(row, batch -> {
                    for (BinaryRow batchRow : batch) {
                        try {
                            sink.write(batchRow);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                rows++;
            }
            out.println("AI Vector Demo Finished. rows=" + rows);
            return rows;
        } finally {
            sink.close();
        }
    }
}
