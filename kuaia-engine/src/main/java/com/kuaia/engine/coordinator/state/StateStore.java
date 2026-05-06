package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.TaskState;
import java.util.List;

public interface StateStore {
    void saveTask(TaskDefinition task, TaskState state);
    void updateTaskState(String taskId, TaskState state);
    TaskState getTaskState(String taskId);
    List<TaskDefinition> getTasksByState(TaskState state);
}
