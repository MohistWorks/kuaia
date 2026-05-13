package com.kuaia.engine.worker;

import com.kuaia.common.rpc.*;
import com.kuaia.common.utils.PendingSet;
import com.kuaia.engine.worker.buffer.RocksDBBuffer;
import com.kuaia.engine.worker.executor.TaskExecutor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkerNode {
    private static final Logger LOG = Logger.getLogger(WorkerNode.class.getName());

    private final String id;
    private final TaskExecutor executor = new TaskExecutor();
    private final RocksDBBuffer dbBuffer = new RocksDBBuffer();
    private final PendingSet pendingSet = new PendingSet();
    private final AtomicInteger memoryQueueSize = new AtomicInteger(0);
    private static final int THRESHOLD = 1000;

    private ManagedChannel channel;
    private StreamObserver<WorkerMessage> requestObserver;
    private volatile boolean stopping;

    public WorkerNode(String id) { this.id = id; }

    public void start(String host, int port) {
        stopping = false;
        try {
            dbBuffer.open("/tmp/kuaia-rocksdb-" + id);
        } catch (Exception e) {
            throw new RuntimeException("Failed to open RocksDB", e);
        }

        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
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
                            LOG.log(Level.INFO, "Spilling to disk: seqId={0}", task.getSeqId());
                            // Since it's spilled, it's no longer in the "memory queue"
                            memoryQueueSize.decrementAndGet();
                        } catch (Exception e) {
                            LOG.log(Level.WARNING, "Failed to spill task to disk: seqId=" + task.getSeqId(), e);
                        }
                    } else {
                        executeTask(task);
                    }
                }
                if (value.hasAssignment()) {
                    completeAssignment(value.getAssignment());
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
                    
                    sendMessage(ackMsg);

                    // Optional: Try to pull from disk if memory is available
                    tryToPullFromDisk();
                });
            }

            private void tryToPullFromDisk() {
                // Simplified "pull back" logic for MVP
                // In a real scenario, we'd need to track which seqIds are on disk
            }

            private void completeAssignment(TaskAssignment assignment) {
                TaskAttemptResult.Builder result = TaskAttemptResult.newBuilder()
                                .setTaskId(assignment.getTaskId())
                                .setAttemptId(assignment.getAttemptId())
                                .setWorkerId(id);
                if (assignment.getTaskId().isEmpty() || assignment.getAttemptId().isEmpty()) {
                    result.setStatus(AttemptStatus.ATTEMPT_FAILED)
                            .setErrorCode("INVALID_ASSIGNMENT")
                            .setErrorMessage("Task assignment requires taskId and attemptId");
                } else {
                    result.setStatus(AttemptStatus.ATTEMPT_SUCCESS);
                }
                WorkerMessage message = WorkerMessage.newBuilder()
                        .setWorkerId(id)
                        .setTaskResult(result.build())
                        .build();
                sendMessage(message);
            }

            @Override public void onError(Throwable t) {
                if (!stopping) {
                    LOG.log(Level.WARNING, "Worker stream failed", t);
                }
            }

            @Override public void onCompleted() {
                if (!stopping) {
                    LOG.info("Worker stream completed");
                }
            }
        });

        sendHello();
    }

    public void stop() {
        stopping = true;
        StreamObserver<WorkerMessage> observer = requestObserver;
        requestObserver = null;
        if (observer != null) {
            synchronized (observer) {
                observer.onCompleted();
            }
        }
        if (channel != null) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
            channel = null;
        }
        dbBuffer.close();
    }

    private void sendSignal(String type) {
        WorkerMessage signal = WorkerMessage.newBuilder()
            .setWorkerId(id)
            .setSignal(ControlSignal.newBuilder().setType(type).build())
            .build();
        sendMessage(signal);
    }

    private void sendHello() {
        WorkerMessage hello = WorkerMessage.newBuilder()
                .setWorkerId(id)
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId(id)
                        .build())
                .build();
        sendMessage(hello);
    }

    private void sendMessage(WorkerMessage message) {
        StreamObserver<WorkerMessage> observer = requestObserver;
        if (observer == null) {
            return;
        }
        synchronized (observer) {
            observer.onNext(message);
        }
    }
}
