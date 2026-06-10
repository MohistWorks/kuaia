package com.kuaia.engine.coordinator.planner;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobSubmissionServiceTest {
    private final InMemoryStateStore store = new InMemoryStateStore();
    private final JobSubmissionService service = new JobSubmissionService(store, new TaskPlanner());

    @Test
    void submitCreatesOneTaskPerBundleAndPersistsJob() {
        JobInstance job = service.submit("job-1", splits(3), 10);

        assertEquals(3, job.getTaskIds().size());
        JobInstance stored = store.getJob("job-1");
        assertEquals(job.getTaskIds(), stored.getTaskIds());
        assertEquals(TaskState.CREATED, stored.getState());
        for (String taskId : stored.getTaskIds()) {
            TaskRecord record = store.getTask(taskId);
            assertNotNull(record);
            assertEquals("job-1", record.getJobId());
            assertEquals(TaskState.CREATED, record.getState());
        }
    }

    @Test
    void submitBundlesSplitsWhenExceedingMaxParallelism() {
        JobInstance job = service.submit("job-1", splits(100), 8);
        // Planner caps tasks at maxParallelism; submission persists exactly that many tasks.
        assertEquals(8, job.getTaskIds().size());
        assertEquals(8, store.getJob("job-1").getTaskIds().size());
    }

    @Test
    void completingAllTasksCascadesJobToCompleted() {
        JobInstance job = service.submit("job-1", splits(2), 10);
        for (String taskId : job.getTaskIds()) {
            completeTask(taskId);
        }
        assertEquals(TaskState.COMPLETED, store.getJob("job-1").getState());
        assertTrue(service.getFailedShards("job-1").isEmpty());
    }

    @Test
    void partialFailureYieldsFinishedWithErrorsAndRecoverableShards() {
        // 3 splits, parallelism 3 -> exactly one split per task, so a failed task maps to one shard.
        JobInstance job = service.submit("job-1", List.of("s0", "s1", "s2"), 3);
        List<String> taskIds = job.getTaskIds();
        completeTask(taskIds.get(0));
        completeTask(taskIds.get(1));
        failTask(taskIds.get(2));

        assertEquals(TaskState.FINISHED_WITH_ERRORS, store.getJob("job-1").getState());
        assertEquals(1, service.getFailedTasks("job-1").size());
        assertEquals(List.of("s2"), service.getFailedShards("job-1"));
    }

    private void completeTask(String taskId) {
        TaskRecord running = store.getTask(taskId).dispatching("w", taskId + "-a", 10_000L).running();
        store.saveTask(running);
        assertTrue(store.compareAndSetTask(running, running.complete(taskId + "-a")));
    }

    private void failTask(String taskId) {
        TaskRecord running = store.getTask(taskId).dispatching("w", taskId + "-a", 10_000L).running();
        store.saveTask(running);
        assertTrue(store.compareAndSetTask(running, running.fail(taskId + "-a", "ERR", "boom")));
    }

    private static List<Object> splits(int count) {
        List<Object> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add("split-" + i);
        }
        return splits;
    }
}
