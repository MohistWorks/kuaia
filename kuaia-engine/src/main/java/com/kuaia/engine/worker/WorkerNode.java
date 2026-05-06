package com.kuaia.engine.worker;

import com.kuaia.common.rpc.*;
import com.kuaia.engine.worker.executor.TaskExecutor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class WorkerNode {
    private final String id;
    private final TaskExecutor executor = new TaskExecutor();
    private StreamObserver<WorkerMessage> requestObserver;

    public WorkerNode(String id) { this.id = id; }

    public void start(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        CoordinatorServiceGrpc.CoordinatorServiceStub stub = CoordinatorServiceGrpc.newStub(channel);

        this.requestObserver = stub.taskStream(new StreamObserver<CoordinatorMessage>() {
            @Override
            public void onNext(CoordinatorMessage value) {
                if (value.hasTask()) {
                    executor.execute(value.getTask()).thenAccept(success -> {
                        WorkerMessage ackMsg = WorkerMessage.newBuilder()
                            .setWorkerId(id)
                            .setAck(TaskAck.newBuilder()
                                .setSeqId(value.getTask().getSeqId())
                                .setSuccess(success)
                                .build())
                            .build();
                        synchronized (requestObserver) {
                            requestObserver.onNext(ackMsg);
                        }
                    });
                }
            }
            @Override public void onError(Throwable t) { t.printStackTrace(); }
            @Override public void onCompleted() { System.out.println("Stream completed"); }
        });
        
        // Initial registration via heartbeat or separate call (omitted for skeleton)
    }
}
