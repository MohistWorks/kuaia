package com.kuaia.engine.ha;

import com.kuaia.common.model.JobInstance;
import com.kuaia.engine.CoordinatorServer;
import com.kuaia.engine.coordinator.state.RatisStateStore;
import com.kuaia.engine.pipeline.PipelineConfig;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Assembles one high-availability coordinator node: a Raft server (this node) + a Raft client over the
 * cluster group backing a {@link RatisStateStore}, wrapped by a {@link CoordinatorServer} whose dispatch
 * loop is gated on this node's Raft leadership. Both the CLI and the failover integration test build a
 * node this way, so leader-gated dispatch is exercised identically in tests and production.
 */
public class HaCoordinator implements AutoCloseable {
    private final String nodeId;
    private final String peers;
    private final File storageDir;
    private final int port;
    private final long leaseMillis;

    private final RaftServer raftServer = new RaftServer();
    private RaftClient raftClient;
    private CoordinatorServer coordinator;
    private boolean closed;

    public HaCoordinator(String nodeId, String peers, File storageDir, int port, long leaseMillis) {
        this.nodeId = nodeId;
        this.peers = peers;
        this.storageDir = storageDir;
        this.port = port;
        this.leaseMillis = leaseMillis;
    }

    /** Start the Raft node, then the gRPC coordinator whose dispatcher only runs while this node leads. */
    public void start() throws Exception {
        raftServer.start(nodeId, peers, storageDir);
        RaftProperties props = new RaftProperties();
        RaftClientConfigKeys.Rpc.setRequestTimeout(props, TimeDuration.valueOf(3, TimeUnit.SECONDS));
        raftClient = RaftClient.newBuilder()
                .setProperties(props)
                .setRaftGroup(raftServer.getGroup())
                .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                        60, TimeDuration.valueOf(500, TimeUnit.MILLISECONDS)))
                .build();
        RatisStateStore store = new RatisStateStore(raftClient);
        coordinator = new CoordinatorServer(store, leaseMillis, raftServer::isLeader);
        coordinator.start(port);
    }

    /** Enumerate splits and persist them as CREATED tasks (routes through Raft to the leader). */
    public JobInstance submit(PipelineConfig pipeline, int maxParallelism) {
        return coordinator.submit(pipeline, maxParallelism);
    }

    /** True when this node is the current Raft leader (and therefore the node that dispatches). */
    public boolean isLeader() {
        return raftServer.isLeader();
    }

    public int port() {
        return coordinator.port();
    }

    public void awaitTermination() throws InterruptedException {
        coordinator.awaitTermination();
    }

    /** Idempotent, leak-safe shutdown: every layer is closed even if an earlier one throws. */
    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (coordinator != null) {
                coordinator.close();
            }
        } finally {
            try {
                if (raftClient != null) {
                    raftClient.close();
                }
            } finally {
                raftServer.close();
            }
        }
    }
}
