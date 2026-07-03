package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.common.rpc.TaskAssignment;
import com.kuaia.common.rpc.TaskPayload;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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

    /**
     * Complete and drop every registered worker stream. A coordinator that lost Raft leadership calls
     * this so its workers re-probe the cluster and land on the new leader.
     */
    public void disconnectAll() {
        for (StreamObserver<CoordinatorMessage> observer : streams.values()) {
            synchronized (observer) {
                try {
                    observer.onCompleted();
                } catch (RuntimeException ignored) {
                    // stream already broken; dropping it is all that matters
                }
            }
        }
        streams.clear();
        pausedWorkers.clear();
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
            TaskAssignment.Builder assignment = TaskAssignment.newBuilder()
                    .setTaskId(task.getTaskId())
                    .setAttemptId(task.getAttemptId())
                    .setStartSeq(task.getLastCheckpointSeq() + 1)
                    .setLeaseUntilMillis(task.getLeaseUntilMillis());
            if (task.getDefinition() != null) {
                assignment.setDefinition(ByteString.copyFrom(serialize(task.getDefinition())));
            }
            synchronized (observer) {
                observer.onNext(CoordinatorMessage.newBuilder().setAssignment(assignment.build()).build());
            }
        }
    }

    private byte[] serialize(Object value) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream objectStream = new ObjectOutputStream(bytes)) {
            objectStream.writeObject(value);
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize task assignment definition", e);
        }
    }
}
