package com.kuaia.common.api;
import com.kuaia.common.data.BinaryRow;

public interface SinkWriter {
    void open() throws Exception;
    void write(BinaryRow row) throws Exception;
    void close() throws Exception;
}
