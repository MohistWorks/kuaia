package com.kuaia.engine;

import com.kuaia.common.rpc.CoordinatorServiceGrpc;
import com.kuaia.common.rpc.JobStatusRequest;
import com.kuaia.common.rpc.JobStatusResponse;
import com.kuaia.common.rpc.SubmitJobRequest;
import com.kuaia.common.rpc.SubmitJobResponse;
import com.kuaia.engine.ha.ClusterAdmin;
import com.kuaia.engine.ha.HaCoordinator;
import com.kuaia.engine.worker.WorkerNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Milestone proof for runtime membership change, sequential 3→4→3→2:
 * a job completes on the initial three nodes; n4 starts idle and is pulled into the quorum via
 * {@link ClusterAdmin#addNode}; a follower is removed and the cluster still serves; the LEADER is
 * removed (leadership auto-transfers first) and the surviving two-node quorum — which necessarily
 * contains n4 — commits one more job, proving the joined node holds fully replicated state. The
 * worker is started once with all four addresses and never touched again.
 */
class DynamicMembershipTest {

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void clusterSurvivesAddFollowerRemovalAndLeaderRemoval(@TempDir Path tmp) throws Exception {
        String[] ids = { "n1", "n2", "n3", "n4" };
        int[] raftPorts = { freePort(), freePort(), freePort(), freePort() };
        int[] grpcPorts = { freePort(), freePort(), freePort(), freePort() };
        String[] raftAddrs = new String[4];
        for (int i = 0; i < 4; i++) {
            raftAddrs[i] = ids[i] + "@127.0.0.1:" + raftPorts[i];
        }
        String peers123 = String.join(",", raftAddrs[0], raftAddrs[1], raftAddrs[2]);
        String peers1234 = String.join(",", raftAddrs);

        Map<String, HaCoordinator> nodes = new LinkedHashMap<>();
        WorkerRunner worker = null;
        try {
            for (int i = 0; i < 3; i++) {
                nodes.put(ids[i], startNode(tmp, ids[i], peers123, grpcPorts[i]));
            }
            assertNotNull(awaitLeader(nodes.values(), 30_000L), "initial leader");

            // Worker gets ALL FOUR addresses up front; n4 is not running yet — probing skips it.
            worker = new WorkerRunner("worker-member-" + System.nanoTime());
            List<WorkerNode.HostPort> workerTargets = new ArrayList<>();
            for (int grpcPort : grpcPorts) {
                workerTargets.add(new WorkerNode.HostPort("127.0.0.1", grpcPort));
            }
            worker.start(workerTargets);

            runJob(tmp, "job-a", currentLeader(nodes.values()));

            // --- add n4: start it idle (peers include itself), then commit the new configuration.
            nodes.put("n4", startNode(tmp, "n4", peers1234, grpcPorts[3]));
            try (ClusterAdmin admin = new ClusterAdmin(peers123, ClusterAdmin.DEFAULT_GROUP_NAME)) {
                assertEquals(4, admin.addNode(raftAddrs[3]).size());
            }
            try (ClusterAdmin admin = new ClusterAdmin(peers1234, ClusterAdmin.DEFAULT_GROUP_NAME)) {
                assertTrue(awaitMembers(admin, 4, 20_000L), "cluster should report 4 members");
            }

            // --- remove a follower among n1–n3, using a DELIBERATELY STALE peer list that omits n4:
            // membership math must be based on the live configuration, so n4 survives and exactly 3
            // members remain (a list-based removal would silently evict n4 and leave 2).
            String leaderAfterAdd;
            try (ClusterAdmin admin = new ClusterAdmin(peers1234, ClusterAdmin.DEFAULT_GROUP_NAME)) {
                leaderAfterAdd = awaitLeaderId(admin, 20_000L);
            }
            String followerId = null;
            for (String candidate : new String[] { "n1", "n2", "n3" }) {
                if (!candidate.equals(leaderAfterAdd)) {
                    followerId = candidate;
                    break;
                }
            }
            try (ClusterAdmin staleAdmin = new ClusterAdmin(peers123, ClusterAdmin.DEFAULT_GROUP_NAME)) {
                assertEquals(3, staleAdmin.removeNode(followerId).size(),
                        "stale peer list must not evict the unlisted member");
            }
            nodes.remove(followerId).close();

            runJob(tmp, "job-b", currentLeader(nodes.values()));

            // --- remove the current leader (leadership auto-transfers inside removeNode). In the
            // rare run where n4 won an election, remove a non-n4 member instead so the final quorum
            // still contains n4 and the replication proof below holds.
            String remainingPeers = peersOf(nodes.keySet(), raftAddrs, ids);
            String toRemove;
            try (ClusterAdmin admin = new ClusterAdmin(remainingPeers, ClusterAdmin.DEFAULT_GROUP_NAME)) {
                toRemove = awaitLeaderId(admin, 20_000L);
                if ("n4".equals(toRemove)) {
                    for (String candidate : nodes.keySet()) {
                        if (!"n4".equals(candidate)) {
                            toRemove = candidate;
                            break;
                        }
                    }
                }
                assertEquals(2, admin.removeNode(toRemove).size());
            }
            nodes.remove(toRemove).close();

            HaCoordinator survivor = awaitLeader(nodes.values(), 30_000L);
            assertNotNull(survivor, "a leader among the final two nodes (one of them n4)");
            assertTrue(nodes.containsKey("n4"), "n4 must be part of the final quorum");

            // Job C committing on the two-node quorum proves n4 replicated the full state.
            runJob(tmp, "job-c", survivor);
        } finally {
            if (worker != null) {
                worker.close();
            }
            for (HaCoordinator node : nodes.values()) {
                node.close();
            }
        }
    }

    private HaCoordinator startNode(Path tmp, String id, String peers, int grpcPort) throws Exception {
        HaCoordinator node = new HaCoordinator(id, peers, new File(tmp.toFile(), id), grpcPort, 5_000L);
        node.start();
        return node;
    }

    /** Submit a small file pipeline on {@code node} and poll it to COMPLETED over real gRPC. */
    private void runJob(Path tmp, String name, HaCoordinator node) throws Exception {
        Path in = tmp.resolve(name + "-in.csv");
        Files.write(in, String.join("\n", "id", "1", "2").getBytes(StandardCharsets.UTF_8));
        Path out = tmp.resolve(name + "-out.csv");
        String yaml = String.join("\n",
                "name: " + name,
                "source:", "  type: file", "  path: " + in, "  format: csv",
                "sink:", "  type: file", "  path: " + out, "  format: csv", "  mode: overwrite");
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", node.port()).usePlaintext().build();
        try {
            CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stub = CoordinatorServiceGrpc.newBlockingStub(channel);
            SubmitJobResponse submit = stub.submitJob(SubmitJobRequest.newBuilder()
                    .setPipelineYaml(yaml).setMaxParallelism(2).build());
            assertTrue(submit.getSuccess(), submit.getError());
            long deadline = System.currentTimeMillis() + 30_000L;
            while (System.currentTimeMillis() < deadline) {
                JobStatusResponse st = stub.getJobStatus(
                        JobStatusRequest.newBuilder().setJobId(submit.getJobId()).build());
                if (st.getFound() && "COMPLETED".equals(st.getJob().getState())) {
                    assertTrue(Files.exists(out), name + " output should exist");
                    return;
                }
                Thread.sleep(200L);
            }
            throw new AssertionError(name + " did not reach COMPLETED in time");
        } finally {
            channel.shutdownNow();
        }
    }

    private static HaCoordinator awaitLeader(Iterable<HaCoordinator> nodes, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            for (HaCoordinator node : nodes) {
                if (node.isLeader()) {
                    return node;
                }
            }
            Thread.sleep(200L);
        }
        return null;
    }

    private static HaCoordinator currentLeader(Iterable<HaCoordinator> nodes) throws InterruptedException {
        HaCoordinator leader = awaitLeader(nodes, 30_000L);
        assertNotNull(leader, "leader should be available");
        return leader;
    }

    private static String awaitLeaderId(ClusterAdmin admin, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            String leaderId = admin.info().leaderId();
            if (leaderId != null) {
                return leaderId;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("no leader visible via cluster info");
    }

    private static boolean awaitMembers(ClusterAdmin admin, int expected, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            ClusterAdmin.ClusterView view = admin.info();
            long reachable = view.members().stream().filter(m -> !"UNREACHABLE".equals(m.role())).count();
            if (view.leaderId() != null && reachable == expected) {
                return true;
            }
            Thread.sleep(200L);
        }
        return false;
    }

    private static String peersOf(Iterable<String> memberIds, String[] raftAddrs, String[] ids) {
        List<String> entries = new ArrayList<>();
        for (String member : memberIds) {
            for (int i = 0; i < ids.length; i++) {
                if (ids[i].equals(member)) {
                    entries.add(raftAddrs[i]);
                }
            }
        }
        return String.join(",", entries);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
