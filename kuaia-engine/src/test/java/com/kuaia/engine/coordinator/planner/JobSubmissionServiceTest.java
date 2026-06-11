package com.kuaia.engine.coordinator.planner;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import com.kuaia.engine.pipeline.ConnectorFactory;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
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

    @Test
    void submitPipelineEnumeratesSplitsAndEmbedsConfig(@TempDir Path tmp) throws Exception {
        Path in = tmp.resolve("in.csv");
        Files.writeString(in, "id\n1\n2\n3\n");
        PipelineConfig cfg = new PipelineConfig(
                "job-1",
                new PipelineConfig.SourceConfig("file", in.toString(), "csv"),
                new PipelineConfig.SinkConfig("file", tmp.resolve("out.csv").toString(), "csv", "overwrite"),
                new PipelineConfig.CheckpointConfig(null));

        InMemoryStateStore store = new InMemoryStateStore();
        JobInstance job = new JobSubmissionService(
                store, new TaskPlanner(),
                new ConnectorFactory(SinkFactoryRegistry.defaultRegistry())).submit("job-1", cfg, 4);

        // 3 data rows fit in a single 10 000-row split, so the planner produces exactly 1 task.
        assertEquals(1, job.getTaskIds().size());
        for (String taskId : job.getTaskIds()) {
            TaskRecord r = store.getTask(taskId);
            assertEquals(TaskState.CREATED, r.getState());
            assertNotNull(r.getDefinition().getConfig().get(JobSubmissionService.PIPELINE_CONFIG_KEY));
            assertTrue(r.getDefinition().getConfig().get(JobSubmissionService.SPLITS_CONFIG_KEY) instanceof List);
        }
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
