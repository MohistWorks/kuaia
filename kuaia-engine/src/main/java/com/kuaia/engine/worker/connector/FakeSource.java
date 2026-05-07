package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.Collector;
import com.kuaia.common.api.SourceReader;
import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;

public class FakeSource implements SourceReader {
    private final KuaiaRowType rowType = new KuaiaRowType(
            new String[]{"id", "name"},
            new DataType[]{DataType.LONG, DataType.STRING}
    );
    private long count = 0;

    @Override public void open() {}
    @Override public void close() {}
    @Override public void pollNext(Collector collector) {
        count++;
        BinaryRow row = new BinaryRow(2);
        row.setLong(0, count);
        row.setString(1, "User-" + count);
        collector.collect(row);
    }
    
    public KuaiaRowType getRowType() { return rowType; }
}
