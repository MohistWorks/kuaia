package com.kuaia.engine.pipeline;

public class PipelineConfigException extends Exception {
    public PipelineConfigException(String message) {
        super(message);
    }

    public PipelineConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
