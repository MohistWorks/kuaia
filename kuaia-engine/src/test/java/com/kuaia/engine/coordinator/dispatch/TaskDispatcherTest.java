package com.kuaia.engine.coordinator.dispatch;

import com.kuaia.common.model.NodeInfo;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.engine.coordinator.planner.JobSubmissionService;
import com.kuaia.engine.coordinator.planner.TaskPlanner;
import com.kuaia.engine.coordinator.recovery.CoordinatorRecoveryPlanner;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.rpc.StreamManager;
import com.kuaia.engine.coordinator.scheduler.Scheduler;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskDispatcherTest {
    private static final long LEASE = 30_000L;

    private final InMemoryStateStore store = new InMemoryStateStore();
    private final WorkerRegistry registry = new WorkerRegistry();
    private final StreamManager streamManager = new StreamManager();
    private final TaskDispatcher dispatcher = new TaskDispatcher(
            store,
            new CoordinatorRecoveryPlanner(store),
            new Scheduler(registry, streamManager),
            streamManager,
            LEASE);

    @Test
    void dispatchesCreatedTasksToAvailableWorker() {
        List<Object> splits = List.of("s0", "s1");
        new JobSubmissionService(store, new TaskPlanner()).submit("job-1", splits, 2);
        CollectingObserver observer = registerWorker("worker-1");

        int dispatched = dispatcher.dispatchOnce(1_000L);

        assertEquals(2, dispatched);
        List<TaskRecord> assigned = store.scanTasksByState(TaskState.DISPATCHING);
        assertEquals(2, assigned.size());
        for (TaskRecord task : assigned) {
            assertEquals("worker-1", task.getAssignedWorkerId());
            assertNotNull(task.getAttemptId());
            assertEquals(1_000L + LEASE, task.getLeaseUntilMillis());
        }
        // The worker stream received one TaskAssignment per task.
        assertEquals(2, observer.assignmentCount());
        assertEquals(1L, observer.messages.get(0).getAssignment().getStartSeq());
    }

    @Test
    void noAvailableWorkerLeavesTasksCreated() {
        new JobSubmissionService(store, new TaskPlanner()).submit("job-1", List.of("s0"), 1);

        int dispatched = dispatcher.dispatchOnce(1_000L);

        assertEquals(0, dispatched);
        assertEquals(1, store.scanTasksByState(TaskState.CREATED).size());
        assertEquals(0, store.scanTasksByState(TaskState.DISPATCHING).size());
    }

    @Test
    void secondDispatchDoesNotReassignAlreadyDispatchedTasks() {
        new JobSubmissionService(store, new TaskPlanner()).submit("job-1", List.of("s0", "s1"), 2);
        CollectingObserver observer = registerWorker("worker-1");

        assertEquals(2, dispatcher.dispatchOnce(1_000L));
        // Second tick within the lease window: nothing is schedulable, no new assignments sent.
        assertEquals(0, dispatcher.dispatchOnce(2_000L));
        assertEquals(2, observer.assignmentCount());
    }

    @Test
    void pausedWorkerIsNotScheduled() {
        new JobSubmissionService(store, new TaskPlanner()).submit("job-1", List.of("s0"), 1);
        registerWorker("worker-1");
        streamManager.setPaused("worker-1", true);

        assertEquals(0, dispatcher.dispatchOnce(1_000L));
        assertEquals(1, store.scanTasksByState(TaskState.CREATED).size());
    }

    @Test
    void expiredLeaseTaskIsRecoveredAndRedispatched() {
        // Seed a RUNNING task whose lease already expired, owned by a worker that is gone.
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("dead-worker", "old-attempt", 500L)
                .running());
        registerWorker("worker-1");

        int dispatched = dispatcher.dispatchOnce(1_000L); // now=1000 > lease=500 -> expired

        assertEquals(1, dispatched);
        TaskRecord task = store.getTask("task-1");
        assertEquals(TaskState.DISPATCHING, task.getState());
        assertEquals("worker-1", task.getAssignedWorkerId());
        assertNotEquals("old-attempt", task.getAttemptId());
        assertEquals(1_000L + LEASE, task.getLeaseUntilMillis());
    }

    @Test
    void followerGateSuppressesDispatch() {
        new JobSubmissionService(store, new TaskPlanner()).submit("job-1", List.of("s0", "s1"), 2);
        registerWorker("worker-1");
        TaskDispatcher follower = new TaskDispatcher(
                store, new CoordinatorRecoveryPlanner(store),
                new Scheduler(registry, streamManager), streamManager, LEASE, () -> false);

        assertEquals(0, follower.dispatchOnce(1_000L));
        assertEquals(2, store.scanTasksByState(TaskState.CREATED).size());
        assertEquals(0, store.scanTasksByState(TaskState.DISPATCHING).size());
    }

    @Test
    void leaderGateAllowsDispatch() {
        new JobSubmissionService(store, new TaskPlanner()).submit("job-1", List.of("s0"), 1);
        registerWorker("worker-1");
        TaskDispatcher leader = new TaskDispatcher(
                store, new CoordinatorRecoveryPlanner(store),
                new Scheduler(registry, streamManager), streamManager, LEASE, () -> true);

        assertEquals(1, leader.dispatchOnce(1_000L));
        assertEquals(1, store.scanTasksByState(TaskState.DISPATCHING).size());
    }

    private CollectingObserver registerWorker(String workerId) {
        registry.register(NodeInfo.builder()
                .id(workerId).host("127.0.0.1").port(9000)
                .type(NodeInfo.NodeType.WORKER).build());
        CollectingObserver observer = new CollectingObserver();
        streamManager.registerStream(workerId, observer);
        return observer;
    }

    /** Captures the CoordinatorMessages the dispatcher pushes to a worker stream. */
    private static final class CollectingObserver implements StreamObserver<CoordinatorMessage> {
        private final List<CoordinatorMessage> messages = new ArrayList<>();

        @Override
        public void onNext(CoordinatorMessage value) {
            messages.add(value);
        }

        long assignmentCount() {
            return messages.stream().filter(CoordinatorMessage::hasAssignment).count();
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
    }
}
