package com.kuaia.engine;

import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.common.rpc.WorkerHello;
import com.kuaia.common.rpc.WorkerMessage;
import com.kuaia.engine.coordinator.dispatch.TaskDispatcher;
import com.kuaia.engine.coordinator.planner.JobSubmissionService;
import com.kuaia.engine.coordinator.planner.TaskPlanner;
import com.kuaia.engine.coordinator.recovery.CoordinatorRecoveryPlanner;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.rpc.CoordinatorServiceImpl;
import com.kuaia.engine.coordinator.rpc.StreamManager;
import com.kuaia.engine.coordinator.rpc.TaskAckHandler;
import com.kuaia.engine.coordinator.scheduler.Scheduler;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import com.kuaia.common.model.TaskState;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end through the real coordinator stream: a worker connects via {@code WorkerHello}, a job is
 * submitted into CREATED tasks, and the dispatch loop assigns them — delivering {@code TaskAssignment}
 * messages to that worker's live stream via the StreamManager shared with the service.
 */
class DispatchIntegrationTest {

    @Test
    void submittedJobIsDispatchedToHelloedWorkerStream() {
        InMemoryStateStore store = new InMemoryStateStore();
        WorkerRegistry registry = new WorkerRegistry();
        StreamManager streamManager = new StreamManager();

        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                registry, null, new TaskAckHandler(store), store, streamManager);
        TaskDispatcher dispatcher = new TaskDispatcher(
                store,
                new CoordinatorRecoveryPlanner(store),
                new Scheduler(registry, streamManager),
                streamManager,
                30_000L);

        // Worker connects and says hello over the bidi stream.
        CollectingObserver workerStream = new CollectingObserver();
        StreamObserver<WorkerMessage> toCoordinator = service.taskStream(workerStream);
        toCoordinator.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1").setHost("127.0.0.1").setPort(9000).build())
                .build());

        // A job is submitted into CREATED tasks, then dispatched.
        new JobSubmissionService(store, new TaskPlanner()).submit("job-1", List.of("s0", "s1"), 2);
        int dispatched = dispatcher.dispatchOnce(1_000L);

        assertEquals(2, dispatched);
        assertEquals(2, store.scanTasksByState(TaskState.DISPATCHING).size());
        assertEquals(2, workerStream.messages.stream().filter(CoordinatorMessage::hasAssignment).count());
        assertTrue(workerStream.messages.stream().allMatch(m ->
                !m.hasAssignment() || "worker-1".equals(store.getTask(m.getAssignment().getTaskId()).getAssignedWorkerId())));
    }

    private static final class CollectingObserver implements StreamObserver<CoordinatorMessage> {
        private final List<CoordinatorMessage> messages = new ArrayList<>();

        @Override
        public void onNext(CoordinatorMessage value) {
            messages.add(value);
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    }
}
