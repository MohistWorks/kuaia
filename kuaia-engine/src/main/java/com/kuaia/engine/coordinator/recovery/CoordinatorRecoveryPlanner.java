package com.kuaia.engine.coordinator.recovery;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.dispatch.TaskRetryPolicy;
import com.kuaia.engine.coordinator.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class CoordinatorRecoveryPlanner {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorRecoveryPlanner.class);

    public static final String LEASE_EXPIRED = "LEASE_EXPIRED";

    private final StateStore stateStore;
    private final int maxAttempts;

    public CoordinatorRecoveryPlanner(StateStore stateStore) {
        this(stateStore, TaskRetryPolicy.DEFAULT_MAX_ATTEMPTS);
    }

    public CoordinatorRecoveryPlanner(StateStore stateStore, int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.stateStore = stateStore;
        this.maxAttempts = maxAttempts;
    }

    public List<TaskRecord> recoverSchedulableTasks(long nowMillis) {
        List<TaskRecord> schedulable = new ArrayList<>();
        schedulable.addAll(stateStore.scanTasksByState(TaskState.CREATED));
        schedulable.addAll(stateStore.scanTasksByState(TaskState.RETRYING));
        recoverExpired(TaskState.DISPATCHING, nowMillis, schedulable);
        recoverExpired(TaskState.RUNNING, nowMillis, schedulable);
        schedulable.sort(Comparator.comparing(TaskRecord::getTaskId));
        return schedulable;
    }

    private void recoverExpired(TaskState state, long nowMillis, List<TaskRecord> schedulable) {
        for (TaskRecord record : stateStore.scanTasksByState(state)) {
            if (record.getLeaseUntilMillis() > nowMillis) {
                continue;
            }
            if (record.getAttemptNo() >= maxAttempts) {
                LOG.warn("Task {} exhausted retries on lease expiry (attempts={}, max={}), failing",
                        record.getTaskId(), record.getAttemptNo(), maxAttempts);
                TaskRecord failed = record.failExhausted(
                        "RETRY_EXHAUSTED", "lease expired; attempts=" + record.getAttemptNo());
                stateStore.compareAndSetTask(record, failed);  // if CAS loses, next pass handles it; not added to schedulable
                continue;
            }
            TaskRecord updated = record.retryingAfterLeaseExpiration(
                    LEASE_EXPIRED,
                    "Task lease expired at " + record.getLeaseUntilMillis());
            if (stateStore.compareAndSetTask(record, updated)) {
                schedulable.add(updated);
            }
        }
    }
}
