package com.kuaia.common.model;

import java.io.Serializable;

public class TaskRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String taskId;
    private final String jobId;
    private final TaskState state;
    private final TaskDefinition definition;
    private final String assignedWorkerId;
    private final String attemptId;
    private final int attemptNo;
    private final long leaseUntilMillis;
    private final long lastCheckpointSeq;
    private final String lastErrorCode;
    private final String lastErrorMessage;
    private final long createdAtMillis;
    private final long updatedAtMillis;
    private final long version;

    private TaskRecord(
            String taskId,
            String jobId,
            TaskState state,
            TaskDefinition definition,
            String assignedWorkerId,
            String attemptId,
            int attemptNo,
            long leaseUntilMillis,
            long lastCheckpointSeq,
            String lastErrorCode,
            String lastErrorMessage,
            long createdAtMillis,
            long updatedAtMillis,
            long version) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.state = state;
        this.definition = definition;
        this.assignedWorkerId = assignedWorkerId;
        this.attemptId = attemptId;
        this.attemptNo = attemptNo;
        this.leaseUntilMillis = leaseUntilMillis;
        this.lastCheckpointSeq = lastCheckpointSeq;
        this.lastErrorCode = lastErrorCode;
        this.lastErrorMessage = lastErrorMessage;
        this.createdAtMillis = createdAtMillis;
        this.updatedAtMillis = updatedAtMillis;
        this.version = version;
    }

    public static TaskRecord created(String jobId, String taskId) {
        return created(jobId, taskId, null);
    }

    public static TaskRecord created(TaskDefinition definition) {
        return fromLegacyState(definition, TaskState.CREATED);
    }

    public static TaskRecord fromLegacyState(TaskDefinition definition, TaskState state) {
        return created(definition.getJobName(), definition.getTaskId(), definition, state);
    }

    private static TaskRecord created(String jobId, String taskId, TaskDefinition definition) {
        return created(jobId, taskId, definition, TaskState.CREATED);
    }

    private static TaskRecord created(String jobId, String taskId, TaskDefinition definition, TaskState state) {
        long now = System.currentTimeMillis();
        return new TaskRecord(
                taskId,
                jobId,
                state,
                definition,
                null,
                null,
                0,
                0L,
                0L,
                null,
                null,
                now,
                now,
                0L);
    }

    public TaskRecord dispatching(String workerId, String attemptId, long leaseUntilMillis) {
        if (state != TaskState.CREATED && state != TaskState.RETRYING) {
            throw new IllegalStateException("Cannot dispatch task from state " + state);
        }
        return copy(
                TaskState.DISPATCHING,
                workerId,
                attemptId,
                attemptNo + 1,
                leaseUntilMillis,
                lastCheckpointSeq,
                lastErrorCode,
                lastErrorMessage);
    }

    public TaskRecord running() {
        if (state != TaskState.DISPATCHING) {
            throw new IllegalStateException("Cannot run task from state " + state);
        }
        return copy(
                TaskState.RUNNING,
                assignedWorkerId,
                attemptId,
                attemptNo,
                leaseUntilMillis,
                lastCheckpointSeq,
                lastErrorCode,
                lastErrorMessage);
    }

    public TaskRecord complete(String attemptId) {
        requireCurrentAttempt(attemptId);
        if (state != TaskState.RUNNING) {
            throw new IllegalStateException("Cannot complete task from state " + state);
        }
        return copy(
                TaskState.COMPLETED,
                assignedWorkerId,
                this.attemptId,
                attemptNo,
                leaseUntilMillis,
                lastCheckpointSeq,
                null,
                null);
    }

    public TaskRecord checkpoint(String attemptId, long processedSeq) {
        requireCurrentAttempt(attemptId);
        if (state != TaskState.RUNNING) {
            throw new IllegalStateException("Cannot checkpoint task from state " + state);
        }
        if (processedSeq <= lastCheckpointSeq) {
            return this;
        }
        return copy(
                state,
                assignedWorkerId,
                this.attemptId,
                attemptNo,
                leaseUntilMillis,
                processedSeq,
                lastErrorCode,
                lastErrorMessage);
    }

    public TaskRecord retrying(String errorCode, String errorMessage) {
        if (state != TaskState.RUNNING) {
            throw new IllegalStateException("Cannot retry task from state " + state);
        }
        return copy(
                TaskState.RETRYING,
                null,
                null,
                attemptNo,
                0L,
                lastCheckpointSeq,
                errorCode,
                errorMessage);
    }

    public TaskRecord retryingAfterLeaseExpiration(String errorCode, String errorMessage) {
        if (state != TaskState.DISPATCHING && state != TaskState.RUNNING) {
            throw new IllegalStateException("Cannot recover expired lease from state " + state);
        }
        return copy(
                TaskState.RETRYING,
                null,
                null,
                attemptNo,
                0L,
                lastCheckpointSeq,
                errorCode,
                errorMessage);
    }

    public TaskRecord withLegacyState(TaskState state) {
        return copy(
                state,
                assignedWorkerId,
                attemptId,
                attemptNo,
                leaseUntilMillis,
                lastCheckpointSeq,
                lastErrorCode,
                lastErrorMessage);
    }

    /**
     * Retry exhausted: terminate to FAILED WITHOUT requiring attempt-id match.
     * For the lease-expiry recovery path when attemptNo has reached the cap.
     * Under normal flow a task reaching this is only ever RUNNING or DISPATCHING.
     */
    public TaskRecord failExhausted(String errorCode, String errorMessage) {
        if (state != TaskState.RUNNING && state != TaskState.DISPATCHING) {
            throw new IllegalStateException("Cannot fail-exhausted task from state " + state);
        }
        return copy(
                TaskState.FAILED,
                assignedWorkerId,
                attemptId,
                attemptNo,
                leaseUntilMillis,
                lastCheckpointSeq,
                errorCode,
                errorMessage);
    }

    public TaskRecord fail(String attemptId, String errorCode, String errorMessage) {
        requireCurrentAttempt(attemptId);
        if (state != TaskState.RUNNING && state != TaskState.RETRYING) {
            throw new IllegalStateException("Cannot fail task from state " + state);
        }
        return copy(
                TaskState.FAILED,
                assignedWorkerId,
                this.attemptId,
                attemptNo,
                leaseUntilMillis,
                lastCheckpointSeq,
                errorCode,
                errorMessage);
    }

    public TaskRecord cancel(String attemptId, String errorMessage) {
        requireCurrentAttempt(attemptId);
        if (state != TaskState.RUNNING && state != TaskState.RETRYING) {
            throw new IllegalStateException("Cannot cancel task from state " + state);
        }
        return copy(
                TaskState.CANCELLED,
                assignedWorkerId,
                this.attemptId,
                attemptNo,
                leaseUntilMillis,
                lastCheckpointSeq,
                "CANCELLED",
                errorMessage);
    }

    private void requireCurrentAttempt(String candidateAttemptId) {
        if (attemptId == null || !attemptId.equals(candidateAttemptId)) {
            throw new IllegalArgumentException("Stale attempt " + candidateAttemptId + " for task " + taskId);
        }
    }

    private TaskRecord copy(
            TaskState state,
            String assignedWorkerId,
            String attemptId,
            int attemptNo,
            long leaseUntilMillis,
            long lastCheckpointSeq,
            String lastErrorCode,
            String lastErrorMessage) {
        return new TaskRecord(
                taskId,
                jobId,
                state,
                definition,
                assignedWorkerId,
                attemptId,
                attemptNo,
                leaseUntilMillis,
                lastCheckpointSeq,
                lastErrorCode,
                lastErrorMessage,
                createdAtMillis,
                System.currentTimeMillis(),
                version + 1);
    }

    public String getTaskId() {
        return taskId;
    }

    public String getJobId() {
        return jobId;
    }

    public TaskState getState() {
        return state;
    }

    public TaskDefinition getDefinition() {
        return definition;
    }

    public String getAssignedWorkerId() {
        return assignedWorkerId;
    }

    public String getAttemptId() {
        return attemptId;
    }

    public int getAttemptNo() {
        return attemptNo;
    }

    public long getLeaseUntilMillis() {
        return leaseUntilMillis;
    }

    public long getLastCheckpointSeq() {
        return lastCheckpointSeq;
    }

    public String getLastErrorCode() {
        return lastErrorCode;
    }

    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public long getUpdatedAtMillis() {
        return updatedAtMillis;
    }

    public long getVersion() {
        return version;
    }
}
