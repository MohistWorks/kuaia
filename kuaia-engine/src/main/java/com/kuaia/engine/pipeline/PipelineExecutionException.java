package com.kuaia.engine.pipeline;

public class PipelineExecutionException extends Exception {
    public PipelineExecutionException(String message) {
        super(message);
    }

    public PipelineExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
