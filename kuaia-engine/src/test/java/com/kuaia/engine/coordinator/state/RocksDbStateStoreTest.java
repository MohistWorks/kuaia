package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RocksDbStateStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void recoversTaskRecordAfterRestart() throws Exception {
        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running()
                .checkpoint("attempt-1", 42L);

        try (RocksDbStateStore store = new RocksDbStateStore(tempDir)) {
            store.saveTask(running);
        }

        try (RocksDbStateStore reopened = new RocksDbStateStore(tempDir)) {
            TaskRecord recovered = reopened.getTask("task-1");

            assertEquals(TaskState.RUNNING, recovered.getState());
            assertEquals("attempt-1", recovered.getAttemptId());
            assertEquals(42L, recovered.getLastCheckpointSeq());
        }
    }

    @Test
    void updatesTaskIndexesAtomicallyWithRecord() throws Exception {
        try (RocksDbStateStore store = new RocksDbStateStore(tempDir)) {
            TaskRecord running = TaskRecord.created("job-1", "task-1")
                    .dispatching("worker-1", "attempt-1", 10_000L)
                    .running();
            store.saveTask(running);

            TaskRecord completed = running.complete("attempt-1");

            assertTrue(store.compareAndSetTask(running, completed));
            assertEquals(1, store.scanTasksByState(TaskState.COMPLETED).size());
            assertEquals(0, store.scanTasksByState(TaskState.RUNNING).size());
            assertEquals(0, store.scanActiveTasksByWorker("worker-1").size());
        }
    }

    @Test
    void retryingTasksAreNotIndexedAsActiveWorkerAssignments() throws Exception {
        try (RocksDbStateStore store = new RocksDbStateStore(tempDir)) {
            store.saveTask(TaskRecord.created("job-1", "task-1")
                    .dispatching("worker-1", "attempt-1", 10_000L)
                    .running()
                    .retrying("TRANSIENT", "temporary failure"));

            assertEquals(0, store.scanActiveTasksByWorker("worker-1").size());
            assertEquals(1, store.scanTasksByState(TaskState.RETRYING).size());
        }
    }

    @Test
    void rejectsCasWhenVersionDoesNotMatch() throws Exception {
        try (RocksDbStateStore store = new RocksDbStateStore(tempDir)) {
            TaskRecord running = TaskRecord.created("job-1", "task-1")
                    .dispatching("worker-1", "attempt-1", 10_000L)
                    .running();
            store.saveTask(running);
            store.compareAndSetTask(running, running.checkpoint("attempt-1", 10L));

            assertFalse(store.compareAndSetTask(running, running.complete("attempt-1")));
            assertEquals(TaskState.RUNNING, store.getTask("task-1").getState());
        }
    }

    @Test
    void persistsAndScansWorkerRecords() throws Exception {
        try (RocksDbStateStore store = new RocksDbStateStore(tempDir)) {
            store.saveWorker(WorkerRecord.registered("worker-1", "127.0.0.1", 9001));
            store.saveWorker(WorkerRecord.registered("worker-2", "127.0.0.1", 9002)
                    .withState(WorkerRecord.WorkerState.OFFLINE));

            List<WorkerRecord> registered = store.scanWorkersByState(WorkerRecord.WorkerState.REGISTERED);

            assertEquals(1, registered.size());
            assertEquals("worker-1", registered.get(0).getWorkerId());
        }
    }
}
