package com.kuaia.engine.coordinator.dispatch;

import com.kuaia.common.model.NodeInfo;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.engine.coordinator.recovery.CoordinatorRecoveryPlanner;
import com.kuaia.engine.coordinator.rpc.StreamManager;
import com.kuaia.engine.coordinator.scheduler.Scheduler;
import com.kuaia.engine.coordinator.state.StateStore;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinator dispatch loop: the driver that turns planned {@code CREATED}/{@code RETRYING} tasks
 * into worker assignments. It is the keystone that connects the Task Planner to the existing
 * scheduler, recovery planner and worker streams — without it those components are never invoked.
 *
 * <p>Each tick: collect the schedulable tasks (via {@link CoordinatorRecoveryPlanner}, which also
 * recovers expired leases), pick a worker (via {@link Scheduler}), atomically transition the task to
 * {@code DISPATCHING} with a fresh attempt and lease (CAS, so a lost race or replay is a no-op), and
 * push a {@code TaskAssignment} to the worker's stream (via {@link StreamManager}).
 *
 * <p>Known limitation (follow-up): assignment within a single tick is greedy — multiple tasks may
 * land on the same least-loaded worker because per-worker load is not bumped intra-tick. Balancing
 * relies on heartbeat/backpressure feedback across ticks.
 */
public class TaskDispatcher implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class);
    private static final long DEFAULT_LEASE_MILLIS = 30_000L;
    private static final long DEFAULT_INTERVAL_MILLIS = 1_000L;

    private final StateStore stateStore;
    private final CoordinatorRecoveryPlanner recoveryPlanner;
    private final Scheduler scheduler;
    private final StreamManager streamManager;
    private final long leaseDurationMillis;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public TaskDispatcher(
            StateStore stateStore,
            CoordinatorRecoveryPlanner recoveryPlanner,
            Scheduler scheduler,
            StreamManager streamManager) {
        this(stateStore, recoveryPlanner, scheduler, streamManager, DEFAULT_LEASE_MILLIS);
    }

    public TaskDispatcher(
            StateStore stateStore,
            CoordinatorRecoveryPlanner recoveryPlanner,
            Scheduler scheduler,
            StreamManager streamManager,
            long leaseDurationMillis) {
        this.stateStore = stateStore;
        this.recoveryPlanner = recoveryPlanner;
        this.scheduler = scheduler;
        this.streamManager = streamManager;
        this.leaseDurationMillis = leaseDurationMillis;
    }

    /** Start the periodic dispatch loop. Per-tick exceptions are logged so the loop never dies. */
    public void start() {
        executor.scheduleAtFixedRate(() -> {
            try {
                dispatchOnce(System.currentTimeMillis());
            } catch (Exception e) {
                LOG.warn("Dispatch tick failed", e);
            }
        }, DEFAULT_INTERVAL_MILLIS, DEFAULT_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * Run a single dispatch pass.
     *
     * @return the number of tasks successfully transitioned to {@code DISPATCHING} and sent.
     */
    public int dispatchOnce(long nowMillis) {
        int dispatched = 0;
        for (TaskRecord task : recoveryPlanner.recoverSchedulableTasks(nowMillis)) {
            Optional<NodeInfo> worker = scheduler.schedule(task.getDefinition());
            if (worker.isEmpty()) {
                continue; // no capacity right now; leave the task for a later tick
            }
            String workerId = worker.get().getId();
            TaskRecord dispatching = task.dispatching(
                    workerId, UUID.randomUUID().toString(), nowMillis + leaseDurationMillis);
            if (stateStore.compareAndSetTask(task, dispatching)) {
                streamManager.sendAssignment(workerId, dispatching);
                dispatched++;
            }
        }
        return dispatched;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
