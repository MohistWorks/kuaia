package com.kuaia.engine.ha;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.rpc.CoordinatorServiceGrpc;
import com.kuaia.common.rpc.JobStatusRequest;
import com.kuaia.common.rpc.JobStatusResponse;
import com.kuaia.common.rpc.ListJobsRequest;
import com.kuaia.common.rpc.ListJobsResponse;
import com.kuaia.engine.pipeline.PipelineConfig;
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

class HaCoordinatorTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void singleNodeHaCoordinatorSubmitsAndListsJob(@TempDir Path tmp) throws Exception {
        int raftPort = freePort();
        int grpcPort = freePort();
        String peers = "n1@127.0.0.1:" + raftPort;
        HaCoordinator ha = new HaCoordinator("n1", peers, new File(tmp.toFile(), "n1"), grpcPort, 30_000L);
        ManagedChannel channel = null;
        try {
            ha.start();
            assertTrue(awaitLeader(ha, 15_000L), "single node should become leader");

            Path in = tmp.resolve("in.csv");
            Files.write(in, String.join("\n", "id", "1", "2").getBytes(StandardCharsets.UTF_8));
            PipelineConfig cfg = new PipelineConfig(
                    "ha-submit",
                    new PipelineConfig.SourceConfig("file", in.toString(), "csv"),
                    new PipelineConfig.SinkConfig("file", tmp.resolve("out.csv").toString(), "csv", "overwrite"),
                    new PipelineConfig.CheckpointConfig(null));
            JobInstance job = ha.submit(cfg, 2);

            channel = ManagedChannelBuilder.forAddress("127.0.0.1", ha.port()).usePlaintext().build();
            CoordinatorServiceGrpc.CoordinatorServiceBlockingStub stub = CoordinatorServiceGrpc.newBlockingStub(channel);

            JobStatusResponse st = stub.getJobStatus(JobStatusRequest.newBuilder().setJobId(job.getJobId()).build());
            assertTrue(st.getFound(), "submitted job should be found via Raft-backed status");

            ListJobsResponse list = stub.listJobs(ListJobsRequest.newBuilder().build());
            assertTrue(list.getJobsList().stream().anyMatch(j -> j.getJobId().equals(job.getJobId())),
                    "listJobs over Raft should include the submitted job");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            ha.close();
        }
    }

    private static boolean awaitLeader(HaCoordinator ha, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (ha.isLeader()) {
                return true;
            }
            Thread.sleep(100L);
        }
        return ha.isLeader();
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
