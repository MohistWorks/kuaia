package com.kuaia.engine.worker;

import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.rpc.*;
import com.kuaia.common.utils.PendingSet;
import com.kuaia.engine.pipeline.ConnectorFactory;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import com.kuaia.engine.worker.buffer.RocksDBBuffer;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import com.kuaia.engine.worker.executor.TaskExecutor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker process core. Connects to the cluster by probing the configured coordinator list in order:
 * each candidate answers the {@code WorkerHello} with a {@code HelloAck}; the worker stays on the
 * coordinator that answers {@code isLeader=true} and moves on when a follower answers {@code false}
 * (or the probe times out / the stream closes). Losing the resident stream — leader crash, or a
 * demoted leader completing its streams — re-enters the same probe loop, with exponential backoff
 * between full failed sweeps, so a worker finds the new leader without operator intervention.
 */
public class WorkerNode {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerNode.class);
    private static final long INITIAL_BACKOFF_MILLIS = 500L;
    private static final long MAX_BACKOFF_MILLIS = 30_000L;
    private static final long HELLO_ACK_TIMEOUT_SECONDS = 5L;

    /** One coordinator candidate address. */
    public record HostPort(String host, int port) {
    }

    private final String id;
    private final TaskExecutor executor = new TaskExecutor();
    private final RocksDBBuffer dbBuffer = new RocksDBBuffer();
    private final PendingSet pendingSet = new PendingSet();
    private final AtomicInteger memoryQueueSize = new AtomicInteger(0);
    private static final int THRESHOLD = 1000;

    private final ExecutorService taskExecutorPool = Executors.newSingleThreadExecutor();
    private final WorkerTaskExecutor taskRunner;

    private volatile List<HostPort> coordinators;
    private volatile ScheduledExecutorService reconnectExecutor;
    private volatile long backoffMillis = INITIAL_BACKOFF_MILLIS;
    private ManagedChannel channel;
    private StreamObserver<WorkerMessage> requestObserver;
    private volatile boolean stopping;

    public WorkerNode(String id) {
        this.id = id;
        this.taskRunner = new WorkerTaskExecutor(
                id,
                new ConnectorFactory(SinkFactoryRegistry.defaultRegistry()),
                EmbeddingProviderRegistry.defaultRegistry(),
                this::sendMessage);
    }

    /** Single-coordinator convenience (single-node deployments and existing callers). */
    public void start(String host, int port) {
        start(List.of(new HostPort(host, port)));
    }

    /**
     * Start the worker against a coordinator cluster. Connection runs asynchronously: the probe loop
     * finds the leader (retrying forever with capped backoff), so this returns immediately.
     */
    public void start(List<HostPort> coordinators) {
        if (coordinators == null || coordinators.isEmpty()) {
            throw new IllegalArgumentException("At least one coordinator address is required");
        }
        stopping = false;
        this.coordinators = List.copyOf(coordinators);
        try {
            dbBuffer.open("/tmp/kuaia-rocksdb-" + id);
        } catch (Exception e) {
            throw new RuntimeException("Failed to open RocksDB", e);
        }
        this.backoffMillis = INITIAL_BACKOFF_MILLIS;
        this.reconnectExecutor = Executors.newSingleThreadScheduledExecutor();
        reconnectExecutor.execute(this::connectLoop);
    }

    /** One sweep over the candidates; on a fully failed sweep, reschedule with doubled (capped) backoff. */
    private void connectLoop() {
        if (stopping) {
            return;
        }
        for (HostPort candidate : coordinators) {
            if (stopping) {
                return;
            }
            if (tryConnect(candidate)) {
                backoffMillis = INITIAL_BACKOFF_MILLIS;
                return;
            }
        }
        long delay = backoffMillis;
        backoffMillis = Math.min(backoffMillis * 2, MAX_BACKOFF_MILLIS);
        LOG.info("Worker {} found no leader among {} coordinators; retrying in {}ms",
                id, coordinators.size(), delay);
        scheduleReconnect(delay);
    }

    private void scheduleReconnect(long delayMillis) {
        ScheduledExecutorService reconnect = reconnectExecutor;
        if (stopping || reconnect == null) {
            return;
        }
        try {
            reconnect.schedule(this::connectLoop, delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            // shutting down
        }
    }

    /**
     * Probe one candidate: open a stream, say hello, and wait for its {@code HelloAck}. Only a
     * {@code isLeader=true} answer makes this the resident connection; anything else (follower answer,
     * timeout, connection failure) closes the probe so the loop can try the next candidate.
     */
    private boolean tryConnect(HostPort candidate) {
        ManagedChannel probeChannel =
                ManagedChannelBuilder.forAddress(candidate.host(), candidate.port()).usePlaintext().build();
        CompletableFuture<Boolean> ack = new CompletableFuture<>();
        try {
            CoordinatorServiceGrpc.CoordinatorServiceStub stub = CoordinatorServiceGrpc.newStub(probeChannel);
            StreamObserver<WorkerMessage> probeObserver = stub.taskStream(coordinatorMessages(ack));
            synchronized (probeObserver) {
                probeObserver.onNext(helloMessage());
            }
            if (Boolean.TRUE.equals(ack.get(HELLO_ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS)) && !stopping) {
                this.channel = probeChannel;
                this.requestObserver = probeObserver;
                LOG.info("Worker {} connected to leader at {}:{}", id, candidate.host(), candidate.port());
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.debug("Worker {} probe of {}:{} failed", id, candidate.host(), candidate.port(), e);
        }
        probeChannel.shutdownNow();
        return false;
    }

    /** Resident stream lost after a successful handshake → drop it and re-enter the probe loop. */
    private void onResidentStreamDown(Throwable cause) {
        if (stopping) {
            return;
        }
        if (cause != null) {
            LOG.warn("Worker {} stream lost; re-probing coordinators", id, cause);
        } else {
            LOG.info("Worker {} stream closed by coordinator; re-probing coordinators", id);
        }
        requestObserver = null;
        ManagedChannel current = channel;
        channel = null;
        if (current != null) {
            current.shutdownNow();
        }
        scheduleReconnect(0L);
    }

    private StreamObserver<CoordinatorMessage> coordinatorMessages(CompletableFuture<Boolean> ack) {
        return new StreamObserver<CoordinatorMessage>() {
            @Override
            public void onNext(CoordinatorMessage value) {
                if (value.hasHelloAck()) {
                    ack.complete(value.getHelloAck().getIsLeader());
                }
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
                            LOG.info("Spilling to disk: seqId={}", task.getSeqId());
                            // Since it's spilled, it's no longer in the "memory queue"
                            memoryQueueSize.decrementAndGet();
                        } catch (Exception e) {
                            LOG.warn("Failed to spill task to disk: seqId={}", task.getSeqId(), e);
                        }
                    } else {
                        executeTask(task);
                    }
                }
                if (value.hasAssignment()) {
                    WorkerNode.this.completeAssignment(value.getAssignment());
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

            @Override
            public void onError(Throwable t) {
                if (!ack.complete(false) && Boolean.TRUE.equals(ack.getNow(false))) {
                    onResidentStreamDown(t);
                }
            }

            @Override
            public void onCompleted() {
                if (!ack.complete(false) && Boolean.TRUE.equals(ack.getNow(false))) {
                    onResidentStreamDown(null);
                }
            }
        };
    }

    public void stop() {
        stopping = true;
        ScheduledExecutorService reconnect = reconnectExecutor;
        reconnectExecutor = null;
        if (reconnect != null) {
            reconnect.shutdownNow();
        }
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
        taskExecutorPool.shutdownNow();
        try {
            taskExecutorPool.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        dbBuffer.close();
    }

    private void completeAssignment(TaskAssignment assignment) {
        if (isInvalidAssignment(assignment)) {
            sendMessage(WorkerMessage.newBuilder()
                    .setWorkerId(id)
                    .setTaskResult(TaskAttemptResult.newBuilder()
                            .setTaskId(assignment.getTaskId())
                            .setAttemptId(assignment.getAttemptId())
                            .setWorkerId(id)
                            .setStatus(AttemptStatus.ATTEMPT_FAILED)
                            .setErrorCode("INVALID_ASSIGNMENT")
                            .setErrorMessage("Task assignment requires taskId, attemptId, definition, and active lease")
                            .build())
                    .build());
            return;
        }
        try {
            taskExecutorPool.submit(() -> taskRunner.execute(assignment));
        } catch (java.util.concurrent.RejectedExecutionException e) {
            LOG.warn("Worker {} rejected task assignment {} during shutdown", id, assignment.getTaskId(), e);
        }
    }

    private boolean isInvalidAssignment(TaskAssignment assignment) {
        return assignment.getTaskId().isEmpty()
                || assignment.getAttemptId().isEmpty()
                || assignment.getLeaseUntilMillis() <= System.currentTimeMillis()
                || !hasMatchingDefinition(assignment);
    }

    private boolean hasMatchingDefinition(TaskAssignment assignment) {
        if (assignment.getDefinition().isEmpty()) {
            return false;
        }
        try (ObjectInputStream objectStream = new ObjectInputStream(
                new ByteArrayInputStream(assignment.getDefinition().toByteArray()))) {
            Object value = objectStream.readObject();
            return value instanceof TaskDefinition
                    && assignment.getTaskId().equals(((TaskDefinition) value).getTaskId());
        } catch (IOException | ClassNotFoundException e) {
            return false;
        }
    }

    private void sendSignal(String type) {
        WorkerMessage signal = WorkerMessage.newBuilder()
            .setWorkerId(id)
            .setSignal(ControlSignal.newBuilder().setType(type).build())
            .build();
        sendMessage(signal);
    }

    private WorkerMessage helloMessage() {
        return WorkerMessage.newBuilder()
                .setWorkerId(id)
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId(id)
                        .build())
                .build();
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
