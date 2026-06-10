package com.kuaia.common.model;

public enum TaskState {
    CREATED,
    DISPATCHING,
    RUNNING,
    COMPLETED,
    RETRYING,
    FAILED,
    CANCELLED,
    FINISHED_WITH_ERRORS
}
