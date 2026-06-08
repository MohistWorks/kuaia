package com.kuaia.engine.coordinator.planner;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskBundle;
import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.StateStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Coordinator entry point that turns a high-level job submission into persisted execution tasks
 * (Task Planner design).
 *
 * <p>It runs the {@link TaskPlanner} to bundle source splits, materializes one {@link TaskRecord}
 * per {@link TaskBundle} — carrying the bundle's splits in the task definition so failed shards stay
 * recoverable — then persists the parent {@link JobInstance} (with its {@code taskIds}) followed by
 * the tasks. Per-task terminal transitions cascade back up to the job's aggregate state via the
 * {@link StateStore}.
 */
public class JobSubmissionService {

    /** Config key under which a task definition carries its bundle's source splits. */
    public static final String SPLITS_CONFIG_KEY = "splits";

    private final StateStore stateStore;
    private final TaskPlanner taskPlanner;

    public JobSubmissionService(StateStore stateStore, TaskPlanner taskPlanner) {
        this.stateStore = stateStore;
        this.taskPlanner = taskPlanner;
    }

    /**
     * Plan {@code sourceSplits} into at most {@code maxParallelism} task bundles, persist them as
     * {@code CREATED} tasks, and persist the parent job.
     *
     * @return the persisted {@link JobInstance} (state {@code CREATED}, with the planned task ids).
     */
    public JobInstance submit(String jobId, List<Object> sourceSplits, int maxParallelism) {
        JobInstance job = new JobInstance();
        job.setJobId(jobId);
        job.setState(TaskState.CREATED);

        List<TaskBundle> bundles = taskPlanner.plan(job, sourceSplits, maxParallelism);

        List<String> taskIds = new ArrayList<>();
        List<TaskRecord> records = new ArrayList<>();
        for (TaskBundle bundle : bundles) {
            TaskDefinition definition = new TaskDefinition();
            definition.setTaskId(bundle.getTaskId());
            definition.setJobName(jobId);
            Map<String, Object> config = new HashMap<>();
            config.put(SPLITS_CONFIG_KEY, bundle.getSplits());
            definition.setConfig(config);

            records.add(TaskRecord.created(definition));
            taskIds.add(bundle.getTaskId());
        }

        job.setTaskIds(taskIds);
        // Persist the job first so a task that terminates immediately can cascade against it.
        stateStore.submitJob(job);
        for (TaskRecord record : records) {
            stateStore.saveTask(record);
        }
        return job;
    }

    /** @return the {@code FAILED} tasks of a (typically {@code FINISHED_WITH_ERRORS}) job. */
    public List<TaskRecord> getFailedTasks(String jobId) {
        JobInstance job = stateStore.getJob(jobId);
        if (job == null || job.getTaskIds() == null) {
            return List.of();
        }
        List<TaskRecord> failed = new ArrayList<>();
        for (String taskId : job.getTaskIds()) {
            TaskRecord record = stateStore.getTask(taskId);
            if (record != null && record.getState() == TaskState.FAILED) {
                failed.add(record);
            }
        }
        return failed;
    }

    /**
     * @return the source splits belonging to a job's failed tasks — the dead-letter shards a user can
     *         re-run partially after a {@code FINISHED_WITH_ERRORS} outcome (design §5).
     */
    public List<Object> getFailedShards(String jobId) {
        List<Object> shards = new ArrayList<>();
        for (TaskRecord record : getFailedTasks(jobId)) {
            TaskDefinition definition = record.getDefinition();
            if (definition == null || definition.getConfig() == null) {
                continue;
            }
            Object splits = definition.getConfig().get(SPLITS_CONFIG_KEY);
            if (splits instanceof List<?>) {
                shards.addAll((List<?>) splits);
            }
        }
        return shards;
    }
}
