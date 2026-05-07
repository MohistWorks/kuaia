package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.KuaiaRowType;

public class MockVectorSink implements SinkWriter {
    private final KuaiaRowType rowType;
    public MockVectorSink(KuaiaRowType rowType) { this.rowType = rowType; }

    @Override public void open() {}
    @Override public void close() {}
    @Override public void write(BinaryRow row) {
        float[] vector = row.getVector(1); // Vector stored in ordinal 1
        System.out.printf("[AI Sink] Row ID: %d, Vector Dim: %d, First Val: %.4f%n", 
            row.getLong(0), vector.length, vector[0]);
    }
}
