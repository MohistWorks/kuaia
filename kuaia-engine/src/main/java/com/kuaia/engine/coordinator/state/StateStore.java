package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public interface StateStore {
    void saveTask(TaskRecord record);

    TaskRecord getTask(String taskId);

    boolean compareAndSetTask(TaskRecord expected, TaskRecord updated);

    List<TaskRecord> scanTasksByState(TaskState state);

    List<TaskRecord> scanActiveTasksByWorker(String workerId);

    void saveWorker(WorkerRecord record);

    WorkerRecord getWorker(String workerId);

    List<WorkerRecord> scanWorkersByState(WorkerRecord.WorkerState state);

    /** Persist a newly planned job (with its {@code taskIds}) so per-task state can cascade up to it. */
    void submitJob(JobInstance job);

    /** @return the stored job instance, or {@code null} if none. */
    JobInstance getJob(String jobId);

    /** @return all persisted jobs (unordered). */
    List<JobInstance> listJobs();

    /** Force the job's aggregate state (e.g. for explicit cancellation); cascade normally maintains it. */
    void updateJobState(String jobId, TaskState state);

    @Deprecated
    default void saveTask(TaskDefinition task, TaskState state) {
        TaskRecord record = TaskRecord.fromLegacyState(task, state);
        saveTask(record);
    }

    @Deprecated
    default void updateTaskState(String taskId, TaskState state) {
        TaskRecord record = getTask(taskId);
        if (record == null) {
            throw new UnsupportedOperationException("Task state updates require an existing TaskRecord");
        }
        if (!compareAndSetTask(record, record.withLegacyState(state))) {
            throw new IllegalStateException("Task " + taskId + " changed while updating state");
        }
    }

    @Deprecated
    default TaskState getTaskState(String taskId) {
        TaskRecord record = getTask(taskId);
        return record == null ? null : record.getState();
    }

    @Deprecated
    default List<TaskDefinition> getTasksByState(TaskState state) {
        List<TaskRecord> records = scanTasksByState(state);
        if (records == null) {
            return Collections.emptyList();
        }
        return records.stream()
                .map(TaskRecord::getDefinition)
                .filter(definition -> definition != null)
                .collect(Collectors.toList());
    }
}
