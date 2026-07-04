package com.kuaia.engine;

import com.kuaia.common.model.JobInstance;
import com.kuaia.engine.coordinator.dispatch.TaskDispatcher;
import com.kuaia.engine.coordinator.planner.JobSubmissionService;
import com.kuaia.engine.coordinator.planner.TaskPlanner;
import com.kuaia.engine.coordinator.recovery.CoordinatorRecoveryPlanner;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.rpc.CoordinatorServiceImpl;
import com.kuaia.engine.coordinator.rpc.StreamManager;
import com.kuaia.engine.coordinator.rpc.TaskAckHandler;
import com.kuaia.engine.coordinator.scheduler.Scheduler;
import com.kuaia.engine.coordinator.state.StateStore;
import com.kuaia.engine.pipeline.ConnectorFactory;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone coordinator process: assembles the state store, worker registry, shared stream manager,
 * gRPC service and dispatch loop, binds a gRPC server, and (optionally) submits a job at startup.
 *
 * <p>Assembly mirrors the proven in-process harness (DispatchIntegrationTest / WorkerExecutionIntegrationTest):
 * the {@link StreamManager} is shared between the service and the {@link TaskDispatcher} so both push
 * assignments to the same live worker streams. Owns the {@link StateStore} and closes it on {@link #close()}.
 */
public class CoordinatorServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorServer.class);

    private final StateStore store;
    private final CoordinatorServiceImpl service;
    private final TaskDispatcher dispatcher;
    private final JobSubmissionService submission;

    private Server server;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public CoordinatorServer(StateStore store, long leaseDurationMillis) {
        this(store, leaseDurationMillis, () -> true);
    }

    public CoordinatorServer(StateStore store, long leaseDurationMillis, BooleanSupplier isLeader) {
        this.store = store;
        WorkerRegistry registry = new WorkerRegistry();
        StreamManager streamManager = new StreamManager();
        this.submission = new JobSubmissionService(
                store, new TaskPlanner(), new ConnectorFactory(SinkFactoryRegistry.defaultRegistry()));
        this.service = new CoordinatorServiceImpl(
                registry, null, new TaskAckHandler(store), store, streamManager, submission, isLeader);
        this.dispatcher = new TaskDispatcher(
                store,
                new CoordinatorRecoveryPlanner(store),
                new Scheduler(registry, streamManager),
                streamManager,
                leaseDurationMillis,
                isLeader);
    }

    /** Enumerate splits from {@code pipeline} and persist them as CREATED tasks. Safe to call before {@link #start}. */
    public JobInstance submit(PipelineConfig pipeline, int maxParallelism) {
        String jobId = pipeline.getName() + "-" + System.currentTimeMillis();
        JobInstance job = submission.submit(jobId, pipeline, maxParallelism);
        LOG.info("Submitted job {} with {} tasks", jobId, job.getTaskIds() == null ? 0 : job.getTaskIds().size());
        return job;
    }

    /** Bind the gRPC server on {@code port} (0 = ephemeral) and start the dispatch loop. */
    public void start(int port) throws IOException {
        this.server = ServerBuilder.forPort(port).addService(service).build().start();
        dispatcher.start();
        LOG.info("Coordinator listening on port {}", server.getPort());
    }

    public int port() {
        return server.getPort();
    }

    public void awaitTermination() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /** Stop dispatch, then the gRPC server, then close the state store. Idempotent. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        dispatcher.close();
        if (server != null) {
            server.shutdown();
            try {
                if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
        }
        if (store instanceof Closeable) {
            try {
                ((Closeable) store).close();
            } catch (IOException e) {
                LOG.warn("Failed to close state store", e);
            }
        }
    }

    /** Package-private accessor for tests to assert persisted job/task state. */
    StateStore stateStore() {
        return store;
    }
}
