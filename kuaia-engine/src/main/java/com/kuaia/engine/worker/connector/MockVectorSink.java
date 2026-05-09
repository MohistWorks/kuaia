package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.io.PrintStream;

public class MockVectorSink implements SinkWriter {
    private final int idOrdinal;
    private final int vectorOrdinal;
    private final PrintStream out;

    public MockVectorSink(KuaiaRowType rowType) throws PipelineExecutionException {
        this(rowType, System.out);
    }

    public MockVectorSink(KuaiaRowType rowType, PrintStream out) throws PipelineExecutionException {
        this.idOrdinal = requireField(rowType, "id", DataType.LONG);
        this.vectorOrdinal = requireField(rowType, "embedding", DataType.VECTOR);
        this.out = out;
    }

    @Override public void open() {}
    @Override public void close() {}
    @Override public void write(BinaryRow row) {
        float[] vector = row.getVector(vectorOrdinal);
        out.printf("[AI Sink] Row ID: %d, Vector Dim: %d, First Val: %.4f%n",
                row.getLong(idOrdinal), vector.length, vector[0]);
    }

    private int requireField(KuaiaRowType rowType, String field, DataType type) throws PipelineExecutionException {
        int ordinal = rowType.getIndex(field);
        if (ordinal < 0 || rowType.getFieldTypes()[ordinal] != type) {
            throw new PipelineExecutionException("Mock vector sink requires " + type.name() + " field: " + field);
        }
        return ordinal;
    }
}
