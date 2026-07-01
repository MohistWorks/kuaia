package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.TaskState;
import com.kuaia.common.rpc.*;
import com.kuaia.engine.coordinator.planner.JobSubmissionService;
import com.kuaia.engine.coordinator.planner.TaskPlanner;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import com.kuaia.engine.pipeline.ConnectorFactory;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CoordinatorRuntimeSubmitTest {

    private static <T> List<T> capture() { return new ArrayList<>(); }

    private CoordinatorServiceImpl service(InMemoryStateStore store) {
        JobSubmissionService submission = new JobSubmissionService(
                store, new TaskPlanner(), new ConnectorFactory(SinkFactoryRegistry.defaultRegistry()));
        return new CoordinatorServiceImpl(
                new WorkerRegistry(), null, new TaskAckHandler(store), store, new StreamManager(), submission);
    }

    private String yaml(Path in, Path out) {
        return String.join("\n",
                "name: rpc-submit-demo",
                "source:", "  type: file", "  path: " + in, "  format: csv",
                "sink:", "  type: file", "  path: " + out, "  format: csv", "  mode: overwrite");
    }

    private <T> StreamObserver<T> observer(List<T> sink) {
        return new StreamObserver<>() {
            @Override public void onNext(T value) { sink.add(value); }
            @Override public void onError(Throwable t) { throw new AssertionError(t); }
            @Override public void onCompleted() { }
        };
    }

    @Test
    void submitPersistsCreatedTasksAndStatusReflectsThem(@TempDir Path tmp) throws Exception {
        Path in = tmp.resolve("in.csv");
        Files.write(in, String.join("\n", "id", "1", "2", "3").getBytes(StandardCharsets.UTF_8));
        InMemoryStateStore store = new InMemoryStateStore();
        CoordinatorServiceImpl svc = service(store);

        List<SubmitJobResponse> submitOut = capture();
        svc.submitJob(SubmitJobRequest.newBuilder().setPipelineYaml(yaml(in, tmp.resolve("out.csv"))).setMaxParallelism(4).build(),
                observer(submitOut));
        assertEquals(1, submitOut.size());
        assertTrue(submitOut.get(0).getSuccess(), submitOut.get(0).getError());
        String jobId = submitOut.get(0).getJobId();
        assertTrue(submitOut.get(0).getTaskCount() > 0);
        assertEquals(TaskState.CREATED, store.getJob(jobId).getState());

        List<JobStatusResponse> statusOut = capture();
        svc.getJobStatus(JobStatusRequest.newBuilder().setJobId(jobId).build(), observer(statusOut));
        assertTrue(statusOut.get(0).getFound());
        assertEquals("CREATED", statusOut.get(0).getJob().getState());
        assertTrue(statusOut.get(0).getJob().getTotalTasks() > 0);

        List<ListJobsResponse> listOut = capture();
        svc.listJobs(ListJobsRequest.newBuilder().build(), observer(listOut));
        assertEquals(1, listOut.get(0).getJobsCount());
        assertEquals(jobId, listOut.get(0).getJobs(0).getJobId());
    }

    @Test
    void submitRejectsMalformedYaml() {
        InMemoryStateStore store = new InMemoryStateStore();
        List<SubmitJobResponse> out = capture();
        service(store).submitJob(
                SubmitJobRequest.newBuilder().setPipelineYaml("not: [valid").setMaxParallelism(0).build(), observer(out));
        assertFalse(out.get(0).getSuccess());
        assertFalse(out.get(0).getError().isEmpty());
    }

    @Test
    void getJobStatusReturnsNotFoundForUnknownJob() {
        List<JobStatusResponse> out = capture();
        service(new InMemoryStateStore()).getJobStatus(
                JobStatusRequest.newBuilder().setJobId("nope").build(), observer(out));
        assertFalse(out.get(0).getFound());
    }

    @Test
    void submitFailsWhenSubmissionNotConfigured() {
        InMemoryStateStore store = new InMemoryStateStore();
        // 5-arg constructor → submission is null.
        CoordinatorServiceImpl svc = new CoordinatorServiceImpl(
                new WorkerRegistry(), null, new TaskAckHandler(store), store, new StreamManager());
        List<SubmitJobResponse> out = capture();
        svc.submitJob(SubmitJobRequest.newBuilder().setPipelineYaml("name: x").build(), observer(out));
        assertFalse(out.get(0).getSuccess());
        assertEquals("submission not configured", out.get(0).getError());
    }
}
