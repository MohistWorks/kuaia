package com.kuaia.engine.coordinator.recovery;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.StateStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class CoordinatorRecoveryPlanner {
    public static final String LEASE_EXPIRED = "LEASE_EXPIRED";

    private final StateStore stateStore;

    public CoordinatorRecoveryPlanner(StateStore stateStore) {
        this.stateStore = stateStore;
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
            TaskRecord updated = record.retryingAfterLeaseExpiration(
                    LEASE_EXPIRED,
                    "Task lease expired at " + record.getLeaseUntilMillis());
            if (stateStore.compareAndSetTask(record, updated)) {
                schedulable.add(updated);
            }
        }
    }
}
