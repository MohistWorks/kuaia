package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.NodeInfo;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.rpc.*;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.state.BatchStateFlusher;
import com.kuaia.engine.coordinator.state.StateStore;
import io.grpc.stub.StreamObserver;

public class CoordinatorServiceImpl extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {
    private final WorkerRegistry registry;
    private final BatchStateFlusher flusher;
    private final TaskAckHandler ackHandler;
    private final StateStore stateStore;
    private final StreamManager streamManager = new StreamManager();

    public CoordinatorServiceImpl(WorkerRegistry registry, BatchStateFlusher flusher) {
        this(registry, flusher, null);
    }

    public CoordinatorServiceImpl(WorkerRegistry registry, BatchStateFlusher flusher, TaskAckHandler ackHandler) {
        this(registry, flusher, ackHandler, null);
    }

    public CoordinatorServiceImpl(
            WorkerRegistry registry,
            BatchStateFlusher flusher,
            TaskAckHandler ackHandler,
            StateStore stateStore) {
        this.registry = registry;
        this.flusher = flusher;
        this.ackHandler = ackHandler;
        this.stateStore = stateStore;
        if (this.flusher != null) {
            this.flusher.start();
        }
    }

    StreamManager getStreamManagerForTesting() {
        return streamManager;
    }

    @Override
    public StreamObserver<WorkerMessage> taskStream(final StreamObserver<CoordinatorMessage> responseObserver) {
        return new StreamObserver<WorkerMessage>() {
            private String workerId;

            @Override
            public void onNext(WorkerMessage value) {
                this.workerId = resolveWorkerId(value);
                if (workerId == null || workerId.isEmpty()) {
                    return;
                }
                streamManager.registerStream(workerId, responseObserver);

                if (value.hasHello()) {
                    registerWorkerHello(value.getHello());
                    replayActiveAssignments(workerId);
                }

                if (value.hasBackpressure()) {
                    boolean paused = value.getBackpressure().getLevel() == BackpressureLevel.BACKPRESSURE_HIGH;
                    streamManager.setPaused(workerId, paused);
                }

                if (value.hasSignal()) {
                    boolean paused = "BACKPRESSURE_HIGH".equals(value.getSignal().getType());
                    streamManager.setPaused(workerId, paused);
                }

                if (value.hasRecordAck() && ackHandler != null) {
                    ackHandler.handleRecordAck(value.getRecordAck());
                }

                if (value.hasCheckpointAck() && ackHandler != null) {
                    ackHandler.handleCheckpointAck(value.getCheckpointAck());
                }

                if (value.hasTaskResult() && ackHandler != null) {
                    ackHandler.handleTaskAttemptResult(value.getTaskResult());
                }

                if (value.hasAck()) {
                    String taskId = value.getAck().getTaskId();
                    if (flusher != null && value.getAck().getSuccess() && taskId != null && !taskId.isEmpty()) {
                        flusher.addAck(taskId);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                streamManager.unregisterStream(workerId);
            }

            @Override
            public void onCompleted() {
                streamManager.unregisterStream(workerId);
            }
        };
    }

    private String resolveWorkerId(WorkerMessage value) {
        if (!value.getWorkerId().isEmpty()) {
            return value.getWorkerId();
        }
        if (value.hasHello()) {
            return value.getHello().getWorkerId();
        }
        if (value.hasHeartbeat()) {
            return value.getHeartbeat().getId();
        }
        if (value.hasRecordAck()) {
            return value.getRecordAck().getWorkerId();
        }
        if (value.hasCheckpointAck()) {
            return value.getCheckpointAck().getWorkerId();
        }
        if (value.hasTaskResult()) {
            return value.getTaskResult().getWorkerId();
        }
        return "";
    }

    private void registerWorkerHello(WorkerHello hello) {
        if (hello.getWorkerId().isEmpty()) {
            return;
        }
        NodeInfo node = NodeInfo.builder()
                .id(hello.getWorkerId())
                .host(hello.getHost())
                .port(hello.getPort())
                .type(NodeInfo.NodeType.WORKER)
                .build();
        registry.register(node);
    }

    private void replayActiveAssignments(String workerId) {
        if (stateStore == null) {
            return;
        }
        for (TaskRecord task : stateStore.scanActiveTasksByWorker(workerId)) {
            streamManager.sendAssignment(workerId, task);
        }
    }

    @Override
    public void registerWorker(WorkerInfo request, StreamObserver<RegistrationResponse> responseObserver) {
        NodeInfo node = NodeInfo.builder()
                .id(request.getId())
                .host(request.getHost())
                .port(request.getPort())
                .type(NodeInfo.NodeType.WORKER)
                .build();
        registry.register(node);
        responseObserver.onNext(RegistrationResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(WorkerHeartbeat request, StreamObserver<HeartbeatResponse> responseObserver) {
        double load = (request.getCpuLoad() + request.getMemLoad()) / 2.0;
        registry.updateHeartbeat(request.getId(), load);
        responseObserver.onNext(HeartbeatResponse.newBuilder().setAck(true).build());
        responseObserver.onCompleted();
    }
}
