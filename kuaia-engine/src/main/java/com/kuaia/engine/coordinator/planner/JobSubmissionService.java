package com.kuaia.engine.coordinator.planner;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskBundle;
import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.StateStore;
import com.kuaia.engine.pipeline.ConnectorFactory;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import com.kuaia.engine.worker.connector.v2.SourceEnumerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(JobSubmissionService.class);

    /**
     * Config key under which a task definition carries its bundle's source splits. The split objects
     * must be {@link java.io.Serializable} — they are persisted with the {@link TaskRecord} via
     * RocksDB / Raft (Java serialization).
     */
    public static final String SPLITS_CONFIG_KEY = "splits";

    /**
     * Config key under which a task definition carries its serialized {@link PipelineConfig}, so the
     * worker can assemble the read/transform/write pipeline. Single source of truth shared with
     * {@code WorkerTaskExecutor}.
     */
    public static final String PIPELINE_CONFIG_KEY = "pipeline";

    private final StateStore stateStore;
    private final TaskPlanner taskPlanner;
    private final ConnectorFactory connectorFactory;

    public JobSubmissionService(StateStore stateStore, TaskPlanner taskPlanner) {
        this(stateStore, taskPlanner, new ConnectorFactory(SinkFactoryRegistry.defaultRegistry()));
    }

    public JobSubmissionService(
            StateStore stateStore, TaskPlanner taskPlanner, ConnectorFactory connectorFactory) {
        this.stateStore = stateStore;
        this.taskPlanner = taskPlanner;
        this.connectorFactory = connectorFactory;
    }

    /**
     * Plan {@code sourceSplits} into at most {@code maxParallelism} task bundles, persist them as
     * {@code CREATED} tasks, and persist the parent job.
     *
     * @return the persisted {@link JobInstance} (state {@code CREATED}, with the planned task ids).
     */
    public JobInstance submit(String jobId, List<Object> sourceSplits, int maxParallelism) {
        return submit(jobId, sourceSplits, maxParallelism, null);
    }

    /**
     * Enumerate the real source splits from {@code pipeline} and submit them, embedding BOTH the
     * splits and the {@link PipelineConfig} into each task definition so the worker can assemble and
     * execute the read/transform/write pipeline.
     *
     * @return the persisted {@link JobInstance} (state {@code CREATED}, with the planned task ids).
     */
    public JobInstance submit(String jobId, PipelineConfig pipeline, int maxParallelism) {
        List<Object> sourceSplits;
        try (SourceEnumerator source = connectorFactory.createSource(pipeline)) {
            source.open();
            sourceSplits = new ArrayList<>(source.enumerateSplits());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to enumerate splits for job " + jobId, e);
        }
        if (sourceSplits.isEmpty()) {
            LOG.warn("Job {} enumerated zero source splits; submitting with no tasks", jobId);
        }
        return submit(jobId, sourceSplits, maxParallelism, pipeline);
    }

    private JobInstance submit(
            String jobId, List<Object> sourceSplits, int maxParallelism, PipelineConfig pipelineOrNull) {
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
            if (pipelineOrNull != null) {
                // Same immutable PipelineConfig instance is shared across all task configs (value object with final fields).
                config.put(PIPELINE_CONFIG_KEY, pipelineOrNull);
            }
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
