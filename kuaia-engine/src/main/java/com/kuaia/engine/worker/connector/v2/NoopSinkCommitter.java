package com.kuaia.engine.worker.connector.v2;

public final class NoopSinkCommitter implements SinkCommitter {
    public static final NoopSinkCommitter INSTANCE = new NoopSinkCommitter();

    private NoopSinkCommitter() {}

    @Override
    public void commit(BatchCommit commit) {}
}
