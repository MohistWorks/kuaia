package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.JobInstance;
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

    @Test
    void recoversJobInstanceAfterRestart() throws Exception {
        JobInstance job = new JobInstance();
        job.setJobId("job-1");
        job.setTaskIds(List.of("task-1", "task-2"));

        try (RocksDbStateStore store = new RocksDbStateStore(tempDir)) {
            store.submitJob(job);
        }

        try (RocksDbStateStore reopened = new RocksDbStateStore(tempDir)) {
            JobInstance recovered = reopened.getJob("job-1");
            assertEquals("job-1", recovered.getJobId());
            assertEquals(TaskState.CREATED, recovered.getState());
            assertEquals(List.of("task-1", "task-2"), recovered.getTaskIds());
        }
    }

    @Test
    void cascadesJobToCompletedWhenAllTasksComplete() throws Exception {
        try (RocksDbStateStore store = new RocksDbStateStore(tempDir)) {
            JobInstance job = new JobInstance();
            job.setJobId("job-1");
            job.setTaskIds(List.of("task-1", "task-2"));
            store.submitJob(job);

            TaskRecord r1 = TaskRecord.created("job-1", "task-1").dispatching("w", "a1", 10_000L).running();
            TaskRecord r2 = TaskRecord.created("job-1", "task-2").dispatching("w", "a2", 10_000L).running();
            store.saveTask(r1);
            store.saveTask(r2);

            assertTrue(store.compareAndSetTask(r1, r1.complete("a1")));
            assertEquals(TaskState.CREATED, store.getJob("job-1").getState());

            assertTrue(store.compareAndSetTask(r2, r2.complete("a2")));
            assertEquals(TaskState.COMPLETED, store.getJob("job-1").getState());
        }
    }

    @Test
    void cascadesJobToFinishedWithErrorsOnPartialFailure() throws Exception {
        try (RocksDbStateStore store = new RocksDbStateStore(tempDir)) {
            JobInstance job = new JobInstance();
            job.setJobId("job-2");
            job.setTaskIds(List.of("task-1", "task-2"));
            store.submitJob(job);

            TaskRecord r1 = TaskRecord.created("job-2", "task-1").dispatching("w", "a1", 10_000L).running();
            TaskRecord r2 = TaskRecord.created("job-2", "task-2").dispatching("w", "a2", 10_000L).running();
            store.saveTask(r1);
            store.saveTask(r2);

            store.compareAndSetTask(r1, r1.complete("a1"));
            store.compareAndSetTask(r2, r2.fail("a2", "ERR", "boom"));

            assertEquals(TaskState.FINISHED_WITH_ERRORS, store.getJob("job-2").getState());
        }
    }
}
