package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import java.util.*;
import java.util.concurrent.*;

public class BatchStateFlusher {
    private final StateStore stateStore;
    private final BlockingQueue<String> ackQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public BatchStateFlusher(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::flushOnceForTesting, 1, 1, TimeUnit.SECONDS);
    }

    public void addAck(String taskId) {
        ackQueue.add(taskId);
    }

    void flushOnceForTesting() {
        List<String> toFlush = new ArrayList<>();
        ackQueue.drainTo(toFlush);
        List<String> retry = new ArrayList<>();
        for (String taskId : toFlush) {
            try {
                completeTask(taskId);
            } catch (Exception e) {
                retry.add(taskId);
            }
        }
        ackQueue.addAll(retry);
    }

    private void completeTask(String taskId) {
        TaskRecord record = stateStore.getTask(taskId);
        if (record == null) {
            stateStore.updateTaskState(taskId, TaskState.COMPLETED);
            return;
        }
        if (record.getState() == TaskState.COMPLETED) {
            return;
        }
        if (record.getState() != TaskState.RUNNING) {
            return;
        }
        TaskRecord completed = record.complete(record.getAttemptId());
        if (!stateStore.compareAndSetTask(record, completed)) {
            throw new IllegalStateException("Task " + taskId + " changed while flushing ack");
        }
    }
}
