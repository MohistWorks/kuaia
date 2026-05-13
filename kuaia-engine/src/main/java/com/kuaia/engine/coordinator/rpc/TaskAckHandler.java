package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.rpc.AttemptStatus;
import com.kuaia.common.rpc.CheckpointAck;
import com.kuaia.common.rpc.RecordAck;
import com.kuaia.common.rpc.TaskAttemptResult;
import com.kuaia.engine.coordinator.state.StateStore;

public class TaskAckHandler {
    private final StateStore stateStore;

    public TaskAckHandler(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    public boolean handleRecordAck(RecordAck ack) {
        TaskRecord record = stateStore.getTask(ack.getTaskId());
        return ack.getSuccess() && isCurrentRunningAttempt(record, ack.getAttemptId(), ack.getWorkerId());
    }

    public boolean handleCheckpointAck(CheckpointAck ack) {
        TaskRecord record = stateStore.getTask(ack.getTaskId());
        if (!isCurrentRunningAttempt(record, ack.getAttemptId(), ack.getWorkerId())) {
            return false;
        }
        try {
            TaskRecord updated = record.checkpoint(ack.getAttemptId(), ack.getProcessedSeq());
            return stateStore.compareAndSetTask(record, updated);
        } catch (IllegalArgumentException | IllegalStateException e) {
            return false;
        }
    }

    public boolean handleTaskAttemptResult(TaskAttemptResult result) {
        TaskRecord record = stateStore.getTask(result.getTaskId());
        if (!isCurrentRunningAttempt(record, result.getAttemptId(), result.getWorkerId())) {
            return false;
        }
        try {
            TaskRecord updated = transitionForResult(record, result);
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

    private boolean isCurrentRunningAttempt(TaskRecord record, String attemptId, String workerId) {
        return record != null
                && record.getState() == TaskState.RUNNING
                && record.getLeaseUntilMillis() > System.currentTimeMillis()
                && attemptId.equals(record.getAttemptId())
                && workerId.equals(record.getAssignedWorkerId());
    }

    private String nullIfEmpty(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}
