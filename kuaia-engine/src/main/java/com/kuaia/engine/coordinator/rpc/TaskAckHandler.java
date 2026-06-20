package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.rpc.AttemptStatus;
import com.kuaia.common.rpc.CheckpointAck;
import com.kuaia.common.rpc.RecordAck;
import com.kuaia.common.rpc.TaskAttemptResult;
import com.kuaia.engine.coordinator.dispatch.TaskRetryPolicy;
import com.kuaia.engine.coordinator.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskAckHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TaskAckHandler.class);

    private final StateStore stateStore;
    private final int maxAttempts;

    public TaskAckHandler(StateStore stateStore) {
        this(stateStore, TaskRetryPolicy.DEFAULT_MAX_ATTEMPTS);
    }

    public TaskAckHandler(StateStore stateStore, int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.stateStore = stateStore;
        this.maxAttempts = maxAttempts;
    }

    public boolean handleRecordAck(RecordAck ack) {
        TaskRecord record = stateStore.getTask(ack.getTaskId());
        // RecordAck is read-only and never changes task state; DISPATCHING->RUNNING promotion is deferred to the first checkpoint or result.
        return ack.getSuccess() && isCurrentAttempt(record, ack.getAttemptId(), ack.getWorkerId());
    }

    public boolean handleCheckpointAck(CheckpointAck ack) {
        TaskRecord record = stateStore.getTask(ack.getTaskId());
        if (!isCurrentAttempt(record, ack.getAttemptId(), ack.getWorkerId())) {
            return false;
        }
        try {
            TaskRecord updated = ensureRunning(record).checkpoint(ack.getAttemptId(), ack.getProcessedSeq());
            return stateStore.compareAndSetTask(record, updated);
        } catch (IllegalArgumentException | IllegalStateException e) {
            return false;
        }
    }

    public boolean handleTaskAttemptResult(TaskAttemptResult result) {
        TaskRecord record = stateStore.getTask(result.getTaskId());
        if (!isCurrentAttempt(record, result.getAttemptId(), result.getWorkerId())) {
            return false;
        }
        try {
            TaskRecord updated = transitionForResult(ensureRunning(record), result);
            return updated != null && stateStore.compareAndSetTask(record, updated);
        } catch (IllegalArgumentException | IllegalStateException e) {
            return false;
        }
    }

    private TaskRecord transitionForResult(TaskRecord record, TaskAttemptResult result) {
        AttemptStatus status = result.getStatus();
        if (status == AttemptStatus.ATTEMPT_SUCCESS) {
            return record.complete(result.getAttemptId());
        }
        if (status == AttemptStatus.ATTEMPT_FAILED) {
            if ("TRANSIENT".equals(result.getErrorCode())) {
                if (record.getAttemptNo() >= maxAttempts) {
                    LOG.warn("Task {} attempt {} exhausted retries (attempts={}, max={}), failing",
                            result.getTaskId(), result.getAttemptId(), record.getAttemptNo(), maxAttempts);
                    return record.fail(
                            result.getAttemptId(),
                            "RETRY_EXHAUSTED",
                            "attempts=" + record.getAttemptNo()
                                    + (result.getErrorMessage().isEmpty() ? "" : "; " + result.getErrorMessage()));
                }
                return record.retrying(
                        nullIfEmpty(result.getErrorCode()),
                        nullIfEmpty(result.getErrorMessage()));
            }
            return record.fail(
                    result.getAttemptId(),
                    nullIfEmpty(result.getErrorCode()),
                    nullIfEmpty(result.getErrorMessage()));
        }
        if (status == AttemptStatus.ATTEMPT_CANCELLED) {
            return record.cancel(result.getAttemptId(), nullIfEmpty(result.getErrorMessage()));
        }
        return null;
    }

    private boolean isCurrentAttempt(TaskRecord record, String attemptId, String workerId) {
        return record != null
                && (record.getState() == TaskState.RUNNING || record.getState() == TaskState.DISPATCHING)
                && record.getLeaseUntilMillis() > System.currentTimeMillis()
                && attemptId.equals(record.getAttemptId())
                && workerId.equals(record.getAssignedWorkerId());
    }

    /** Promote DISPATCHING -> RUNNING; pass through if already RUNNING. */
    private TaskRecord ensureRunning(TaskRecord record) {
        return record.getState() == TaskState.DISPATCHING ? record.running() : record;
    }

    private String nullIfEmpty(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}
