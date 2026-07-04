package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.common.rpc.WorkerHello;
import com.kuaia.common.rpc.WorkerMessage;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Leadership handshake on the worker stream: a leader answers {@code HelloAck(true)} and keeps the
 * stream; a follower answers {@code HelloAck(false)}, completes the stream, and runs no
 * registration/replay — the worker moves on to probe the next coordinator.
 */
class CoordinatorHelloAckTest {

    private static final class CollectingObserver implements StreamObserver<CoordinatorMessage> {
        private final List<CoordinatorMessage> messages = new ArrayList<>();
        private boolean completed;

        @Override public void onNext(CoordinatorMessage value) { messages.add(value); }
        @Override public void onError(Throwable t) { throw new AssertionError(t); }
        @Override public void onCompleted() { completed = true; }
    }

    private static CoordinatorServiceImpl service(InMemoryStateStore store, StreamManager streams, BooleanSupplier isLeader) {
        return new CoordinatorServiceImpl(
                new WorkerRegistry(), null, new TaskAckHandler(store), store, streams, null, isLeader);
    }

    private static WorkerMessage hello(String workerId) {
        return WorkerMessage.newBuilder()
                .setWorkerId(workerId)
                .setHello(WorkerHello.newBuilder().setWorkerId(workerId).setHost("127.0.0.1").setPort(9000).build())
                .build();
    }

    @Test
    void leaderAcksHelloAndKeepsStream() {
        InMemoryStateStore store = new InMemoryStateStore();
        StreamManager streams = new StreamManager();
        CollectingObserver worker = new CollectingObserver();

        service(store, streams, () -> true).taskStream(worker).onNext(hello("w1"));

        assertFalse(worker.messages.isEmpty(), "leader should answer the hello");
        CoordinatorMessage first = worker.messages.get(0);
        assertTrue(first.hasHelloAck());
        assertTrue(first.getHelloAck().getIsLeader());
        assertFalse(worker.completed, "leader keeps the worker stream open");
        assertTrue(streams.isAvailable("w1"), "leader registers the worker stream");
    }

    @Test
    void followerRejectsHelloAndCompletesStream() {
        InMemoryStateStore store = new InMemoryStateStore();
        StreamManager streams = new StreamManager();
        CollectingObserver worker = new CollectingObserver();

        service(store, streams, () -> false).taskStream(worker).onNext(hello("w1"));

        assertEquals(1, worker.messages.size(), "follower answers exactly the ack");
        assertTrue(worker.messages.get(0).hasHelloAck());
        assertFalse(worker.messages.get(0).getHelloAck().getIsLeader());
        assertTrue(worker.completed, "follower completes the stream so the worker probes on");
        assertFalse(streams.isAvailable("w1"), "follower does not keep the worker registered");
    }

    @Test
    void followerDoesNotReplayActiveAssignments() {
        InMemoryStateStore store = new InMemoryStateStore();
        // Seed an in-flight task owned by the worker with a live lease: a leader would replay it.
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("w1", "attempt-1", System.currentTimeMillis() + 60_000L));
        StreamManager streams = new StreamManager();
        CollectingObserver worker = new CollectingObserver();

        service(store, streams, () -> false).taskStream(worker).onNext(hello("w1"));

        assertTrue(worker.messages.stream().noneMatch(CoordinatorMessage::hasAssignment),
                "no replay runs on a follower");
    }

    @Test
    void staleStreamTerminationDoesNotEvictReconnectedWorker() {
        InMemoryStateStore store = new InMemoryStateStore();
        StreamManager streams = new StreamManager();
        CoordinatorServiceImpl svc = service(store, streams, () -> true);

        // First connection: worker w1 registers on the leader.
        CollectingObserver oldStream = new CollectingObserver();
        StreamObserver<WorkerMessage> oldRequest = svc.taskStream(oldStream);
        oldRequest.onNext(hello("w1"));

        // The worker reconnects (new stream, same id) before the old stream's death is noticed.
        CollectingObserver newStream = new CollectingObserver();
        svc.taskStream(newStream).onNext(hello("w1"));
        assertTrue(streams.isAvailable("w1"));

        // The old stream's late terminal callback must NOT evict the fresh registration.
        oldRequest.onError(new RuntimeException("stale connection finally died"));
        assertTrue(streams.isAvailable("w1"), "reconnected worker must stay schedulable");
    }

    @Test
    void leaderReplaysActiveAssignmentsAfterAck() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("w1", "attempt-1", System.currentTimeMillis() + 60_000L));
        StreamManager streams = new StreamManager();
        CollectingObserver worker = new CollectingObserver();

        service(store, streams, () -> true).taskStream(worker).onNext(hello("w1"));

        assertTrue(worker.messages.get(0).hasHelloAck(), "ack arrives before any replay");
        assertTrue(worker.messages.stream().anyMatch(CoordinatorMessage::hasAssignment),
                "leader replays the in-flight assignment");
    }
}
