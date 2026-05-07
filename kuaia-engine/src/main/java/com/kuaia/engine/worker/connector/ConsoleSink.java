package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.KuaiaRowType;

public class ConsoleSink implements SinkWriter {
    private final KuaiaRowType rowType;

    public ConsoleSink(KuaiaRowType rowType) { this.rowType = rowType; }

    @Override public void open() {}
    @Override public void close() {}
    @Override public void write(BinaryRow row) {
        StringBuilder sb = new StringBuilder("[Kuaia] Row: ");
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            sb.append(rowType.getFieldNames()[i]).append("=");
            switch (rowType.getFieldTypes()[i]) {
                case LONG: sb.append(row.getLong(i)); break;
                case STRING: sb.append(row.getString(i)); break;
                default: sb.append("?");
            }
            if (i < rowType.getFieldNames().length - 1) sb.append(", ");
        }
        System.out.println(sb.toString());
    }
}
