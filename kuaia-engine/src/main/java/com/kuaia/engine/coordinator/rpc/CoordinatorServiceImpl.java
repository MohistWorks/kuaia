package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.NodeInfo;
import com.kuaia.common.rpc.*;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import io.grpc.stub.StreamObserver;

public class CoordinatorServiceImpl extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {
    private final WorkerRegistry registry;

    public CoordinatorServiceImpl(WorkerRegistry registry) {
        this.registry = registry;
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
