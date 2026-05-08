package com.kuaia.engine.worker;

import com.kuaia.common.rpc.*;
import com.kuaia.common.utils.PendingSet;
import com.kuaia.engine.worker.buffer.RocksDBBuffer;
import com.kuaia.engine.worker.executor.TaskExecutor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicInteger;

public class WorkerNode {
    private final String id;
    private final TaskExecutor executor = new TaskExecutor();
    private final RocksDBBuffer dbBuffer = new RocksDBBuffer();
    private final PendingSet pendingSet = new PendingSet();
    private final AtomicInteger memoryQueueSize = new AtomicInteger(0);
    private static final int THRESHOLD = 1000;

    private StreamObserver<WorkerMessage> requestObserver;

    public WorkerNode(String id) { this.id = id; }

    public void start(String host, int port) {
        try {
            dbBuffer.open("/tmp/kuaia-rocksdb-" + id);
        } catch (Exception e) {
            throw new RuntimeException("Failed to open RocksDB", e);
        }

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        CoordinatorServiceGrpc.CoordinatorServiceStub stub = CoordinatorServiceGrpc.newStub(channel);

        this.requestObserver = stub.taskStream(new StreamObserver<CoordinatorMessage>() {
            @Override
            public void onNext(CoordinatorMessage value) {
                if (value.hasTask()) {
                    TaskPayload task = value.getTask();
                    pendingSet.add(task.getSeqId());
                    int currentSize = memoryQueueSize.incrementAndGet();

                    if (currentSize > THRESHOLD * 0.8) {
                        sendSignal("BACKPRESSURE_HIGH");
                    }

                    if (currentSize > THRESHOLD) {
                        try {
                            dbBuffer.put(task.getSeqId(), task.toByteArray());
                            System.out.println("Spilling to disk: seqId=" + task.getSeqId());
                            // Since it's spilled, it's no longer in the "memory queue"
                            memoryQueueSize.decrementAndGet();
                        } catch (Exception e) {
                            System.err.println("Failed to spill task to disk: " + e.getMessage());
                        }
                    } else {
                        executeTask(task);
                    }
                }
            }

            private void executeTask(TaskPayload task) {
                executor.execute(task).thenAccept(success -> {
                    memoryQueueSize.decrementAndGet();
                    if (memoryQueueSize.get() < THRESHOLD * 0.3) {
                        sendSignal("BACKPRESSURE_LOW");
                    }
                    pendingSet.remove(task.getSeqId());

                    WorkerMessage ackMsg = WorkerMessage.newBuilder()
                        .setWorkerId(id)
                        .setAck(TaskAck.newBuilder()
                            .setSeqId(task.getSeqId())
                            .setSuccess(success)
                            .setTaskId(task.getTaskId())
                            .build())
                        .build();
                    
                    synchronized (requestObserver) {
                        requestObserver.onNext(ackMsg);
                    }

                    // Optional: Try to pull from disk if memory is available
                    tryToPullFromDisk();
                });
            }

            private void tryToPullFromDisk() {
                // Simplified "pull back" logic for MVP
                // In a real scenario, we'd need to track which seqIds are on disk
            }

            @Override public void onError(Throwable t) { t.printStackTrace(); }
            @Override public void onCompleted() { System.out.println("Stream completed"); }
        });
        
        // Initial registration via heartbeat or separate call (omitted for skeleton)
    }

    private void sendSignal(String type) {
        WorkerMessage signal = WorkerMessage.newBuilder()
            .setWorkerId(id)
            .setSignal(ControlSignal.newBuilder().setType(type).build())
            .build();
        synchronized (requestObserver) {
            requestObserver.onNext(signal);
        }
    }
}
