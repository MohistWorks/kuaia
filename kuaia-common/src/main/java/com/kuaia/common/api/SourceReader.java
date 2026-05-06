package com.kuaia.common.api;

public interface SourceReader {
    void open() throws Exception;
    void pollNext(Collector collector) throws Exception;
    void close() throws Exception;
}
