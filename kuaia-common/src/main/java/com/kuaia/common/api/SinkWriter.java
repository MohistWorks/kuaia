package com.kuaia.common.api;
import com.kuaia.common.data.BinaryRow;

import java.util.List;

public interface SinkWriter {
    void open() throws Exception;
    void write(BinaryRow row) throws Exception;
    default void writeBatch(List<BinaryRow> rows) throws Exception {
        for (BinaryRow row : rows) {
            write(row);
        }
    }
    void close() throws Exception;
}
