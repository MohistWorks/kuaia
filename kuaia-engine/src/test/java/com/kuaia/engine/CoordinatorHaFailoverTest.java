package com.kuaia.engine;

import com.kuaia.common.rpc.CoordinatorServiceGrpc;
import com.kuaia.common.rpc.JobStatusRequest;
import com.kuaia.common.rpc.JobStatusResponse;
import com.kuaia.common.rpc.ListJobsRequest;
import com.kuaia.common.rpc.ListJobsResponse;
import com.kuaia.common.rpc.SubmitJobRequest;
import com.kuaia.common.rpc.SubmitJobResponse;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Milestone proof: 3 HA coordinators replicate state through Raft; only the leader dispatches. The
 * worker is started ONCE with the full coordinator list and never touched again: it probes for the
 * leader, a job runs to COMPLETED, the leader is killed, a new leader is elected, the replicated job
 * state survives, and the worker re-probes on its own so the new leader dispatches a fresh job to it.
 */
class CoordinatorHaFailoverTest {

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void jobSurvivesLeaderFailoverAndNewLeaderDispatches(@TempDir Path tmp) throws Exception {
        String[] ids = { "n1", "n2", "n3" };
        int[] raftPorts = { freePort(), freePort(), freePort() };
        int[] grpcPorts = { freePort(), freePort(), freePort() };
        String peers = String.join(",",
                "n1@127.0.0.1:" + raftPorts[0],
                "n2@127.0.0.1:" + raftPorts[1],
                "n3@127.0.0.1:" + raftPorts[2]);

        List<HaCoordinator> nodes = new ArrayList<>();
        WorkerRunner worker = null;
        ManagedChannel channel = null;
        try {
            for (int i = 0; i < 3; i++) {
                HaCoordinator ha = new HaCoordinator(
                        ids[i], peers, new File(tmp.toFile(), ids[i]), grpcPorts[i], 5_000L);
                ha.start();
                nodes.add(ha);
            }

            HaCoordinator leader = awaitLeader(nodes, 30_000L);
            assertNotNull(leader, "a leader should be elected");

            // The worker gets the FULL coordinator list and finds the leader itself; after this line
            // the worker is never touched again — auto-discovery must carry it across the failover.
            worker = new WorkerRunner("worker-ha-" + System.nanoTime());
            worker.start(List.of(
                    new WorkerNode.HostPort("127.0.0.1", grpcPorts[0]),
                    new WorkerNode.HostPort("127.0.0.1", grpcPorts[1]),
                    new WorkerNode.HostPort("127.0.0.1", grpcPorts[2])));

            // Submit job A via runtime RPC on the leader; run to COMPLETED.
            Path inA = tmp.resolve("inA.csv");
            Files.write(inA, String.join("\n", "id", "1", "2", "3").getBytes(StandardCharsets.UTF_8));
            Path outA = tmp.resolve("outA.csv");

            channel = plaintext(leader.port());
            CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stub = CoordinatorServiceGrpc.newBlockingStub(channel);
            SubmitJobResponse subA = stub.submitJob(SubmitJobRequest.newBuilder()
                    .setPipelineYaml(yaml("ha-job-a", inA, outA)).setMaxParallelism(4).build());
            assertTrue(subA.getSuccess(), subA.getError());
            String jobA = subA.getJobId();
            assertTrue(awaitStatus(stub, jobA, "COMPLETED", 30_000L), "job A should complete on the leader");
            channel.shutdownNow();
            channel = null;

            // Kill the leader; a new leader is elected among the survivors.
            leader.close();
            nodes.remove(leader);
            HaCoordinator newLeader = awaitLeader(nodes, 30_000L);
            assertNotNull(newLeader, "a new leader should be elected after failover");

            // Replicated state survived: job A is still visible on the new leader (status + listJobs).
            channel = plaintext(newLeader.port());
            CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stub2 = CoordinatorServiceGrpc.newBlockingStub(channel);
            assertTrue(awaitStatus(stub2, jobA, "COMPLETED", 15_000L),
                    "job A state should survive failover on the new leader");
            ListJobsResponse list = stub2.listJobs(ListJobsRequest.newBuilder().build());
            assertTrue(list.getJobsList().stream().anyMatch(j -> j.getJobId().equals(jobA)),
                    "listJobs over Raft should include job A after failover");

            // Submit job B on the new leader; the worker re-probes and reconnects ON ITS OWN.
            Path inB = tmp.resolve("inB.csv");
            Files.write(inB, String.join("\n", "id", "9", "8").getBytes(StandardCharsets.UTF_8));
            Path outB = tmp.resolve("outB.csv");
            SubmitJobResponse subB = stub2.submitJob(SubmitJobRequest.newBuilder()
                    .setPipelineYaml(yaml("ha-job-b", inB, outB)).setMaxParallelism(4).build());
            assertTrue(subB.getSuccess(), subB.getError());
            assertTrue(awaitStatus(stub2, subB.getJobId(), "COMPLETED", 30_000L),
                    "job B should complete on the new leader after failover");
            assertTrue(Files.exists(outB), "job B output should exist");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            if (worker != null) {
                worker.close();
            }
            for (HaCoordinator ha : nodes) {
                ha.close();
            }
        }
    }

    private static String yaml(String name, Path in, Path out) {
        return String.join("\n",
                "name: " + name,
                "source:", "  type: file", "  path: " + in, "  format: csv",
                "sink:", "  type: file", "  path: " + out, "  format: csv", "  mode: overwrite");
    }

    private static ManagedChannel plaintext(int port) {
        return ManagedChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
    }

    private static HaCoordinator awaitLeader(List<HaCoordinator> nodes, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            for (HaCoordinator ha : nodes) {
                if (ha.isLeader()) {
                    return ha;
                }
            }
            Thread.sleep(200L);
        }
        return null;
    }

    private static boolean awaitStatus(
            CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stub,
            String jobId, String state, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            JobStatusResponse st = stub.getJobStatus(JobStatusRequest.newBuilder().setJobId(jobId).build());
            if (st.getFound() && state.equals(st.getJob().getState())) {
                return true;
            }
            Thread.sleep(200L);
        }
        return false;
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
