package com.kuaia.engine.worker.connector.v2;

public interface SinkCommitter {
    void commit(BatchCommit commit) throws Exception;
}
