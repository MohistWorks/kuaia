package com.kuaia.engine.coordinator.state;

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

    @Deprecated
    default void saveTask(TaskDefinition task, TaskState state) {
        TaskRecord record = TaskRecord.created(task.getJobName(), task.getTaskId());
        saveTask(record);
    }

    @Deprecated
    default void updateTaskState(String taskId, TaskState state) {
        throw new UnsupportedOperationException("Task state updates require a TaskRecord in the v2 state model");
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
