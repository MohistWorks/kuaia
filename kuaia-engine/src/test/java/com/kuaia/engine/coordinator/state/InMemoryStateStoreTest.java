package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryStateStoreTest {
    @Test
    void legacySaveTaskPreservesTaskDefinitionForStateScans() {
        InMemoryStateStore store = new InMemoryStateStore();
        TaskDefinition definition = new TaskDefinition();
        definition.setTaskId("task-1");
        definition.setJobName("job-1");
        definition.setConfig(Collections.singletonMap("source", "file"));

        store.saveTask(definition, TaskState.CREATED);

        List<TaskDefinition> definitions = store.getTasksByState(TaskState.CREATED);
        assertEquals(1, definitions.size());
        assertEquals("task-1", definitions.get(0).getTaskId());
        assertEquals("file", definitions.get(0).getConfig().get("source"));
        assertEquals(definition, store.getTask("task-1").getDefinition());
    }

    @Test
    void legacySaveTaskHonorsRequestedState() {
        InMemoryStateStore store = new InMemoryStateStore();
        TaskDefinition definition = new TaskDefinition();
        definition.setTaskId("task-1");
        definition.setJobName("job-1");
        definition.setConfig(Collections.singletonMap("source", "file"));

        store.saveTask(definition, TaskState.FAILED);

        assertEquals(TaskState.FAILED, store.getTask("task-1").getState());
        assertEquals(1, store.getTasksByState(TaskState.FAILED).size());
        assertEquals(0, store.getTasksByState(TaskState.CREATED).size());
        assertEquals(definition, store.getTask("task-1").getDefinition());
    }

    @Test
    void legacyUpdateTaskStateUpdatesExistingTaskRecord() {
        InMemoryStateStore store = new InMemoryStateStore();
        TaskDefinition definition = new TaskDefinition();
        definition.setTaskId("task-1");
        definition.setJobName("job-1");
        store.saveTask(definition, TaskState.CREATED);

        store.updateTaskState("task-1", TaskState.FAILED);

        assertEquals(TaskState.FAILED, store.getTask("task-1").getState());
        assertEquals(TaskState.FAILED, store.getTaskState("task-1"));
        assertEquals(1, store.getTasksByState(TaskState.FAILED).size());
        assertEquals(0, store.getTasksByState(TaskState.CREATED).size());
        assertEquals(definition, store.getTask("task-1").getDefinition());
    }

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
    void retryingTasksAreNotActiveWorkerAssignments() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running()
                .retrying("TRANSIENT", "temporary failure"));

        assertEquals(0, store.scanActiveTasksByWorker("worker-1").size());
        assertEquals(1, store.scanTasksByState(TaskState.RETRYING).size());
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

    @Test
    void submitGetAndUpdateJobState() {
        InMemoryStateStore store = new InMemoryStateStore();
        JobInstance job = new JobInstance();
        job.setJobId("job-1");
        job.setTaskIds(List.of("task-1"));
        store.submitJob(job);

        assertEquals("job-1", store.getJob("job-1").getJobId());
        assertEquals(TaskState.CREATED, store.getJob("job-1").getState());

        store.updateJobState("job-1", TaskState.CANCELLED);
        assertEquals(TaskState.CANCELLED, store.getJob("job-1").getState());
    }

    @Test
    void cascadesJobToCompletedWhenAllTasksComplete() {
        InMemoryStateStore store = new InMemoryStateStore();
        JobInstance job = new JobInstance();
        job.setJobId("job-1");
        job.setTaskIds(List.of("task-1", "task-2"));
        store.submitJob(job);

        TaskRecord r1 = TaskRecord.created("job-1", "task-1").dispatching("w", "a1", 10_000L).running();
        TaskRecord r2 = TaskRecord.created("job-1", "task-2").dispatching("w", "a2", 10_000L).running();
        store.saveTask(r1);
        store.saveTask(r2);

        assertTrue(store.compareAndSetTask(r1, r1.complete("a1")));
        // task-2 still running -> job not yet finalized.
        assertEquals(TaskState.CREATED, store.getJob("job-1").getState());

        assertTrue(store.compareAndSetTask(r2, r2.complete("a2")));
        assertEquals(TaskState.COMPLETED, store.getJob("job-1").getState());
    }

    @Test
    void cascadesJobToFinishedWithErrorsOnPartialFailure() {
        InMemoryStateStore store = new InMemoryStateStore();
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
