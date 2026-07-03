package com.kuaia.engine;

import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.RatisStateStore;
import com.kuaia.engine.ha.RaftServer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

public class CoordinatorHAIntegrationTest {
    @TempDir
    Path tempDir;

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testHADistributedState() throws Exception {
        RaftClusterFixture fixture = startThreeNodeCluster();
        RaftClient client = null;
        RaftServer leader = null;

        try {
            RaftProperties properties = new RaftProperties();
            RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(2, TimeUnit.SECONDS));
            client = RaftClient.newBuilder()
                    .setProperties(properties)
                    .setRaftGroup(fixture.group)
                    .build();

            RatisStateStore store = new RatisStateStore(client);
            waitForLeader(fixture.groupId, 10_000L, fixture.servers);

            TaskDefinition task = new TaskDefinition();
            task.setTaskId("t1");
            store.saveTask(task, TaskState.CREATED);
            eventuallyAssertTaskState(store, "t1", TaskState.CREATED, 5_000L);

            leader = waitForLeader(fixture.groupId, 5_000L, fixture.servers);
            leader.close();
            fixture.closedLeader = leader;

            eventuallyAssertTaskState(store, "t1", TaskState.CREATED, 10_000L);
        } finally {
            if (client != null) {
                client.close();
            }
            fixture.close();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void staleLeaderClientWriteFailsAfterLeaderShutdown() throws Exception {
        RaftClusterFixture fixture = startThreeNodeCluster();
        RaftServer leader = waitForLeader(fixture.groupId, 10_000L, fixture.servers);
        RaftPeerId oldLeaderId = leader.getInternalServer().getId();
        RaftPeer oldLeaderPeer = fixture.peer(oldLeaderId);

        leader.close();
        fixture.closedLeader = leader;
        waitForLeader(fixture.groupId, 10_000L, fixture.openServers());

        RaftProperties properties = new RaftProperties();
        RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(1, TimeUnit.SECONDS));
        try (RaftClient staleClient = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(RaftGroup.valueOf(fixture.groupId, oldLeaderPeer))
                .setLeaderId(oldLeaderId)
                .setRetryPolicy(RetryPolicies.noRetry())
                .build()) {
            RatisStateStore store = new RatisStateStore(staleClient);

            assertThrows(RuntimeException.class, () -> store.updateTaskState("stale-write", TaskState.CREATED));
        } finally {
            fixture.close();
        }
    }

    private RaftClusterFixture startThreeNodeCluster() throws Exception {
        int p1Port = findFreePort();
        int p2Port = findFreePort();
        int p3Port = findFreePort();
        String p1Address = "127.0.0.1:" + p1Port;
        String p2Address = "127.0.0.1:" + p2Port;
        String p3Address = "127.0.0.1:" + p3Port;
        RaftPeer p1 = RaftPeer.newBuilder().setId("p1").setAddress(p1Address).build();
        RaftPeer p2 = RaftPeer.newBuilder().setId("p2").setAddress(p2Address).build();
        RaftPeer p3 = RaftPeer.newBuilder().setId("p3").setAddress(p3Address).build();
        RaftPeer[] peers = new RaftPeer[] { p1, p2, p3 };
        RaftServer[] servers = new RaftServer[] { new RaftServer(), new RaftServer(), new RaftServer() };
        String allPeers = String.join(",",
                "p1@" + p1Address, "p2@" + p2Address, "p3@" + p3Address);
        TimeDuration min = TimeDuration.valueOf(150, TimeUnit.MILLISECONDS);
        TimeDuration max = TimeDuration.valueOf(300, TimeUnit.MILLISECONDS);
        servers[0].start("p1", allPeers, new File(tempDir.toFile(), "n1"), min, max, "test-group");
        servers[1].start("p2", allPeers, new File(tempDir.toFile(), "n2"), min, max, "test-group");
        servers[2].start("p3", allPeers, new File(tempDir.toFile(), "n3"), min, max, "test-group");
        RaftGroupId groupId = servers[0].getGroupId();
        RaftGroup group = servers[0].getGroup();
        return new RaftClusterFixture(groupId, group, peers, servers);
    }

    private RaftServer waitForLeader(RaftGroupId groupId, long timeoutMillis, RaftServer... servers) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            for (RaftServer server : servers) {
                try {
                    if (server.getInternalServer().getDivision(groupId).getInfo().isLeader()) {
                        return server;
                    }
                } catch (Exception ignored) {
                    // Division can be unavailable while Ratis is still starting.
                }
            }
            Thread.sleep(100L);
        }
        fail("Timed out waiting for Raft leader within " + timeoutMillis + "ms");
        return null;
    }

    private void eventuallyAssertTaskState(
            RatisStateStore store,
            String taskId,
            TaskState expected,
            long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Throwable lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                assertEquals(expected, store.getTaskState(taskId));
                return;
            } catch (Throwable t) {
                lastFailure = t;
                Thread.sleep(100L);
            }
        }
        AssertionError error = new AssertionError(
                "Timed out waiting for task " + taskId + " to reach " + expected + " within " + timeoutMillis + "ms");
        if (lastFailure != null) {
            error.initCause(lastFailure);
        }
        throw error;
    }

    private int findFreePort() throws Exception {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static class RaftClusterFixture implements AutoCloseable {
        private final RaftGroupId groupId;
        private final RaftGroup group;
        private final RaftPeer[] peers;
        private final RaftServer[] servers;
        private RaftServer closedLeader;

        private RaftClusterFixture(RaftGroupId groupId, RaftGroup group, RaftPeer[] peers, RaftServer[] servers) {
            this.groupId = groupId;
            this.group = group;
            this.peers = peers;
            this.servers = servers;
        }

        private RaftServer[] openServers() {
            return Arrays.stream(servers)
                    .filter(server -> server != closedLeader)
                    .toArray(RaftServer[]::new);
        }

        private RaftPeer peer(RaftPeerId peerId) {
            return Arrays.stream(peers)
                    .filter(peer -> peer.getId().equals(peerId))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing peer " + peerId));
        }

        @Override
        public void close() throws Exception {
            for (RaftServer server : servers) {
                if (server != closedLeader) {
                    server.close();
                }
            }
        }
    }
}
