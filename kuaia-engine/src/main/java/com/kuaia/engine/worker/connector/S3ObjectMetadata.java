package com.kuaia.engine.worker.connector;

final class S3ObjectMetadata {
    private final String key;
    private final long size;

    S3ObjectMetadata(String key, long size) {
        this.key = key;
        this.size = size;
    }

    String getKey() {
        return key;
    }

    long getSize() {
        return size;
    }
}
