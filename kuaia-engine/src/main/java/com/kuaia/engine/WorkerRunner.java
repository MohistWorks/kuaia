package com.kuaia.engine;

import com.kuaia.engine.worker.WorkerNode;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone worker process: wraps a {@link WorkerNode}, connects it to a coordinator, and blocks in
 * {@link #awaitTermination()} until {@link #close()} (driven by a JVM shutdown hook in the CLI).
 */
public class WorkerRunner implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerRunner.class);

    private final WorkerNode worker;
    private final CountDownLatch terminated = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public WorkerRunner(String workerId) {
        this.worker = new WorkerNode(workerId);
    }

    public void start(String host, int port) {
        start(List.of(new WorkerNode.HostPort(host, port)));
    }

    /** Probe the coordinator list for the current leader and stay on it (see {@link WorkerNode}). */
    public void start(List<WorkerNode.HostPort> coordinators) {
        LOG.info("Worker connecting to coordinators {}", coordinators);
        worker.start(coordinators);
    }

    public void awaitTermination() throws InterruptedException {
        terminated.await();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        worker.stop();
        terminated.countDown();
    }
}
