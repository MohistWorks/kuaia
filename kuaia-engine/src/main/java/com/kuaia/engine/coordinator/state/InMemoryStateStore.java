package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InMemoryStateStore implements StateStore {
    private final Map<String, TaskRecord> tasks = new ConcurrentHashMap<>();
    private final Map<String, WorkerRecord> workers = new ConcurrentHashMap<>();

    @Override
    public void saveTask(TaskRecord record) {
        tasks.put(record.getTaskId(), record);
    }

    @Override
    public TaskRecord getTask(String taskId) {
        return tasks.get(taskId);
    }

    @Override
    public boolean compareAndSetTask(TaskRecord expected, TaskRecord updated) {
        if (!expected.getTaskId().equals(updated.getTaskId())) {
            return false;
        }
        return tasks.compute(expected.getTaskId(), (taskId, current) -> {
            if (current == null || current.getVersion() != expected.getVersion()) {
                return current;
            }
            return updated;
        }) == updated;
    }

    @Override
    public List<TaskRecord> scanTasksByState(TaskState state) {
        return tasks.values().stream()
                .filter(record -> record.getState() == state)
                .sorted(Comparator.comparing(TaskRecord::getTaskId))
                .collect(Collectors.toList());
    }

    @Override
    public List<TaskRecord> scanActiveTasksByWorker(String workerId) {
        List<TaskRecord> result = new ArrayList<>();
        for (TaskRecord record : tasks.values()) {
            if (workerId.equals(record.getAssignedWorkerId()) && isActive(record.getState())) {
                result.add(record);
            }
        }
        result.sort(Comparator.comparing(TaskRecord::getTaskId));
        return result;
    }

    @Override
    public void saveWorker(WorkerRecord record) {
        workers.put(record.getWorkerId(), record);
    }

    @Override
    public WorkerRecord getWorker(String workerId) {
        return workers.get(workerId);
    }

    @Override
    public List<WorkerRecord> scanWorkersByState(WorkerRecord.WorkerState state) {
        return workers.values().stream()
                .filter(record -> record.getState() == state)
                .sorted(Comparator.comparing(WorkerRecord::getWorkerId))
                .collect(Collectors.toList());
    }

    private boolean isActive(TaskState state) {
        return state == TaskState.DISPATCHING || state == TaskState.RUNNING || state == TaskState.RETRYING;
    }
}
