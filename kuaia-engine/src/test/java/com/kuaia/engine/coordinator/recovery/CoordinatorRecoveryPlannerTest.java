package com.kuaia.engine.coordinator.recovery;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorRecoveryPlannerTest {
    @TempDir
    Path tempDir;

    @Test
    void returnsCreatedAndRetryingTasksAsSchedulable() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-created"));
        store.saveTask(TaskRecord.created("job-1", "task-retrying")
                .dispatching("worker-1", "attempt-1", 10L)
                .running()
                .retrying("TRANSIENT", "temporary failure"));
        store.saveTask(TaskRecord.created("job-1", "task-completed")
                .dispatching("worker-1", "attempt-1", 10L)
                .running()
                .complete("attempt-1"));

        List<TaskRecord> recovered = new CoordinatorRecoveryPlanner(store).recoverSchedulableTasks(100L);

        assertEquals("task-created,task-retrying", taskIds(recovered));
        assertEquals(TaskState.CREATED, store.getTask("task-created").getState());
        assertEquals(TaskState.RETRYING, store.getTask("task-retrying").getState());
    }

    @Test
    void expiredRunningTaskMovesToRetryingAndPreservesCheckpoint() {
        InMemoryStateStore store = new InMemoryStateStore();
        TaskRecord running = TaskRecord.created("job-1", "task-running")
                .dispatching("worker-1", "attempt-1", 50L)
                .running()
                .checkpoint("attempt-1", 42L);
        store.saveTask(running);

        List<TaskRecord> recovered = new CoordinatorRecoveryPlanner(store).recoverSchedulableTasks(100L);

        assertEquals(1, recovered.size());
        TaskRecord stored = store.getTask("task-running");
        assertEquals(TaskState.RETRYING, stored.getState());
        assertEquals(42L, stored.getLastCheckpointSeq());
        assertEquals("LEASE_EXPIRED", stored.getLastErrorCode());
        assertNull(stored.getAssignedWorkerId());
        assertNull(stored.getAttemptId());
        assertEquals(running.getAttemptNo(), stored.getAttemptNo());
        assertEquals(running.getVersion() + 1, stored.getVersion());
    }

    @Test
    void unexpiredRunningTaskIsNotRecovered() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-running")
                .dispatching("worker-1", "attempt-1", 500L)
                .running());

        List<TaskRecord> recovered = new CoordinatorRecoveryPlanner(store).recoverSchedulableTasks(100L);

        assertTrue(recovered.isEmpty());
        assertEquals(TaskState.RUNNING, store.getTask("task-running").getState());
    }

    @Test
    void expiredDispatchingTaskMovesToRetrying() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-dispatching")
                .dispatching("worker-1", "attempt-1", 50L));

        List<TaskRecord> recovered = new CoordinatorRecoveryPlanner(store).recoverSchedulableTasks(100L);

        assertEquals("task-dispatching", taskIds(recovered));
        TaskRecord stored = store.getTask("task-dispatching");
        assertEquals(TaskState.RETRYING, stored.getState());
        assertEquals("LEASE_EXPIRED", stored.getLastErrorCode());
        assertNull(stored.getAssignedWorkerId());
        assertNull(stored.getAttemptId());
    }

    @Test
    void recoversExpiredRunningTaskAfterRocksDbRestart() throws Exception {
        TaskRecord running = TaskRecord.created("job-1", "task-running")
                .dispatching("worker-1", "attempt-1", 50L)
                .running()
                .checkpoint("attempt-1", 7L);

        try (RocksDbStateStore store = new RocksDbStateStore(tempDir)) {
            store.saveTask(running);
        }

        try (RocksDbStateStore reopened = new RocksDbStateStore(tempDir)) {
            List<TaskRecord> recovered = new CoordinatorRecoveryPlanner(reopened).recoverSchedulableTasks(100L);

            assertEquals("task-running", taskIds(recovered));
            TaskRecord stored = reopened.getTask("task-running");
            assertEquals(TaskState.RETRYING, stored.getState());
            assertEquals(7L, stored.getLastCheckpointSeq());
            assertNull(stored.getAssignedWorkerId());
            assertNull(stored.getAttemptId());
        }
    }

    @Test
    void expiredTaskBelowCapBecomesRetrying() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "a1", 500L)   // lease 500 already expired vs now=1000
                .running());
        CoordinatorRecoveryPlanner planner = new CoordinatorRecoveryPlanner(store, 4);

        List<TaskRecord> schedulable = planner.recoverSchedulableTasks(1_000L);

        assertEquals(TaskState.RETRYING, store.getTask("task-1").getState());
        assertTrue(schedulable.stream().anyMatch(t -> t.getTaskId().equals("task-1")));
    }

    @Test
    void expiredTaskAtCapBecomesFailed() {
        InMemoryStateStore store = new InMemoryStateStore();
        TaskRecord r = TaskRecord.created("job-1", "task-1");
        for (int i = 1; i <= 3; i++) {
            r = r.dispatching("worker-1", "a" + i, 500L).running().retrying("X", "x");
        }
        r = r.dispatching("worker-1", "a4", 500L).running();  // attemptNo == 4, lease 500 expired
        store.saveTask(r);
        CoordinatorRecoveryPlanner planner = new CoordinatorRecoveryPlanner(store, 4);

        List<TaskRecord> schedulable = planner.recoverSchedulableTasks(1_000L);

        assertEquals(TaskState.FAILED, store.getTask("task-1").getState());
        assertEquals("RETRY_EXHAUSTED", store.getTask("task-1").getLastErrorCode());
        assertTrue(schedulable.stream().noneMatch(t -> t.getTaskId().equals("task-1")));
    }

    private String taskIds(List<TaskRecord> records) {
        return records.stream()
                .map(TaskRecord::getTaskId)
                .collect(Collectors.joining(","));
    }
}
