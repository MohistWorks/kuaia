package com.kuaia.engine.worker.connector;

import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.List;

interface S3ObjectStore extends AutoCloseable {
    List<S3ObjectMetadata> listObjects(String bucket, String prefix) throws PipelineExecutionException;

    String readUtf8Object(String bucket, String key) throws PipelineExecutionException;

    @Override
    void close() throws Exception;
}
