package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryStateStoreTest {
    @Test
    void scansTasksByState() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1"));
        store.saveTask(TaskRecord.created("job-1", "task-2")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running());

        List<TaskRecord> created = store.scanTasksByState(TaskState.CREATED);

        assertEquals(1, created.size());
        assertEquals("task-1", created.get(0).getTaskId());
    }

    @Test
    void scansActiveTasksByWorker() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running());

        List<TaskRecord> active = store.scanActiveTasksByWorker("worker-1");

        assertEquals(1, active.size());
        assertEquals("attempt-1", active.get(0).getAttemptId());
    }

    @Test
    void savesAndScansWorkerRecordsByState() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveWorker(WorkerRecord.registered("worker-1", "127.0.0.1", 9001));
        store.saveWorker(WorkerRecord.registered("worker-2", "127.0.0.1", 9002)
                .withState(WorkerRecord.WorkerState.OFFLINE));

        List<WorkerRecord> registered = store.scanWorkersByState(WorkerRecord.WorkerState.REGISTERED);

        assertEquals(1, registered.size());
        assertEquals("worker-1", registered.get(0).getWorkerId());
    }
}
