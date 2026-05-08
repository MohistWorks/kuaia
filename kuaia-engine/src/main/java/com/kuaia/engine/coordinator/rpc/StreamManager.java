package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.common.rpc.TaskAssignment;
import com.kuaia.common.rpc.TaskPayload;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamManager {
    private final Map<String, StreamObserver<CoordinatorMessage>> streams = new ConcurrentHashMap<>();
    private final Map<String, Boolean> pausedWorkers = new ConcurrentHashMap<>();

    public void registerStream(String workerId, StreamObserver<CoordinatorMessage> observer) {
        streams.put(workerId, observer);
        pausedWorkers.putIfAbsent(workerId, false);
    }

    public void unregisterStream(String workerId) {
        if (workerId != null) {
            streams.remove(workerId);
            pausedWorkers.remove(workerId);
        }
    }

    public void setPaused(String workerId, boolean paused) {
        pausedWorkers.put(workerId, paused);
    }

    public boolean isAvailable(String workerId) {
        return streams.containsKey(workerId) && !pausedWorkers.getOrDefault(workerId, false);
    }

    public void sendTask(String workerId, TaskPayload task) {
        StreamObserver<CoordinatorMessage> observer = streams.get(workerId);
        if (observer != null) {
            synchronized (observer) {
                observer.onNext(CoordinatorMessage.newBuilder().setTask(task).build());
            }
        }
    }

    public void sendAssignment(String workerId, TaskRecord task) {
        StreamObserver<CoordinatorMessage> observer = streams.get(workerId);
        if (observer != null) {
            TaskAssignment assignment = TaskAssignment.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setAttemptId(task.getAttemptId())
                    .setStartSeq(task.getLastCheckpointSeq() + 1)
                    .setLeaseUntilMillis(task.getLeaseUntilMillis())
                    .build();
            synchronized (observer) {
                observer.onNext(CoordinatorMessage.newBuilder().setAssignment(assignment).build());
            }
        }
    }
}
