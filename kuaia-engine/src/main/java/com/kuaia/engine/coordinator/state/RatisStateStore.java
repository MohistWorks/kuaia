package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.TaskState;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class RatisStateStore implements StateStore {
    // In a real implementation, this would send Raft messages.
    // For the skeleton, we use a thread-safe map that the StateMachine will eventually manage.
    private final Map<String, TaskDefinition> tasks = new ConcurrentHashMap<>();
    private final Map<String, TaskState> states = new ConcurrentHashMap<>();

    @Override
    public void saveTask(TaskDefinition task, TaskState state) {
        tasks.put(task.getTaskId(), task);
        states.put(task.getTaskId(), state);
    }

    @Override
    public void updateTaskState(String taskId, TaskState state) {
        states.put(taskId, state);
    }

    @Override
    public TaskState getTaskState(String taskId) {
        return states.get(taskId);
    }

    @Override
    public List<TaskDefinition> getTasksByState(TaskState state) {
        List<TaskDefinition> result = new ArrayList<>();
        for (Map.Entry<String, TaskState> entry : states.entrySet()) {
            if (entry.getValue() == state) {
                result.add(tasks.get(entry.getKey()));
            }
        }
        return result;
    }
}
