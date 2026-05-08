package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.KuaiaRowType;

import java.io.PrintStream;

public class MockVectorSink implements SinkWriter {
    private final KuaiaRowType rowType;
    private final int vectorOrdinal;
    private final PrintStream out;

    public MockVectorSink(KuaiaRowType rowType) {
        this(rowType, System.out);
    }

    public MockVectorSink(KuaiaRowType rowType, PrintStream out) {
        this.rowType = rowType;
        int embeddingIndex = rowType.getIndex("embedding");
        this.vectorOrdinal = embeddingIndex >= 0 ? embeddingIndex : 1;
        this.out = out;
    }

    @Override public void open() {}
    @Override public void close() {}
    @Override public void write(BinaryRow row) {
        float[] vector = row.getVector(vectorOrdinal);
        out.printf("[AI Sink] Row ID: %d, Vector Dim: %d, First Val: %.4f%n",
                row.getLong(0), vector.length, vector[0]);
    }
}
