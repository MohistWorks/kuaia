package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.JobStateEvaluator;
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
    private final Map<String, JobInstance> jobs = new ConcurrentHashMap<>();

    @Override
    public void saveTask(TaskRecord record) {
        TaskRecord previous = tasks.put(record.getTaskId(), record);
        applyJobCounterDelta(previous == null ? null : previous.getState(), record);
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
        TaskState[] oldState = new TaskState[1];
        boolean applied = tasks.compute(expected.getTaskId(), (taskId, current) -> {
            if (current == null || current.getVersion() != expected.getVersion()) {
                return current;
            }
            oldState[0] = current.getState();
            return updated;
        }) == updated;
        if (applied) {
            applyJobCounterDelta(oldState[0], updated);
        }
        return applied;
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

    @Override
    public void submitJob(JobInstance job) {
        jobs.put(job.getJobId(), job);
    }

    @Override
    public JobInstance getJob(String jobId) {
        return jobs.get(jobId);
    }

    @Override
    public void updateJobState(String jobId, TaskState state) {
        jobs.computeIfPresent(jobId, (id, job) -> {
            job.setState(state);
            return job;
        });
    }

    /**
     * Maintain the parent job's terminal-task counters incrementally for a task transitioning from
     * {@code oldState} to {@code newRecord.getState()}. Mirrors the Raft state machine's cascade so
     * this in-memory store stays a faithful test double. No-op when the state is unchanged or there is
     * no terminal cross, keeping it idempotent.
     */
    private void applyJobCounterDelta(TaskState oldState, TaskRecord newRecord) {
        if (newRecord.getJobId() == null) {
            return;
        }
        TaskState newState = newRecord.getState();
        if (oldState == newState) {
            return;
        }
        boolean oldTerminal = isTerminal(oldState);
        boolean newTerminal = isTerminal(newState);
        if (!oldTerminal && !newTerminal) {
            return;
        }
        jobs.computeIfPresent(newRecord.getJobId(), (id, job) -> {
            if (job.getTaskIds() == null) {
                return job;
            }
            if (oldTerminal) {
                adjustBucket(job, oldState, -1);
            }
            if (newTerminal) {
                adjustBucket(job, newState, 1);
            }
            JobStateEvaluator.evaluate(
                    job.getTaskIds().size(),
                    job.getCompletedTasks(),
                    job.getFailedTasks(),
                    job.getCancelledTasks()).ifPresent(job::setState);
            return job;
        });
    }

    /** Terminal task states are those a task rests in: COMPLETED, FAILED, CANCELLED. */
    private boolean isTerminal(TaskState state) {
        return state != null && !JobStateEvaluator.isActive(state);
    }

    private void adjustBucket(JobInstance job, TaskState state, int delta) {
        switch (state) {
            case COMPLETED -> job.setCompletedTasks(job.getCompletedTasks() + delta);
            case FAILED -> job.setFailedTasks(job.getFailedTasks() + delta);
            case CANCELLED -> job.setCancelledTasks(job.getCancelledTasks() + delta);
            default -> { /* non-terminal states have no bucket */ }
        }
    }

    private boolean isActive(TaskState state) {
        return state == TaskState.DISPATCHING || state == TaskState.RUNNING;
    }
}
