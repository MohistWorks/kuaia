package com.kuaia.engine;

import com.google.protobuf.ByteString;
import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.common.rpc.TaskAssignment;
import com.kuaia.common.rpc.WorkerHello;
import com.kuaia.common.rpc.WorkerMessage;
import com.kuaia.engine.coordinator.dispatch.TaskDispatcher;
import com.kuaia.engine.coordinator.planner.JobSubmissionService;
import com.kuaia.engine.coordinator.planner.TaskPlanner;
import com.kuaia.engine.coordinator.recovery.CoordinatorRecoveryPlanner;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.rpc.CoordinatorServiceImpl;
import com.kuaia.engine.coordinator.rpc.StreamManager;
import com.kuaia.engine.coordinator.rpc.TaskAckHandler;
import com.kuaia.engine.coordinator.scheduler.Scheduler;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import com.kuaia.engine.pipeline.ConnectorFactory;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import com.kuaia.engine.worker.WorkerTaskExecutor;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import com.kuaia.engine.worker.connector.v2.SourceEnumerator;
import com.kuaia.engine.worker.connector.v2.SourceSplit;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Milestone end-to-end test: a real {@code file -> identity -> file} job is submitted, dispatched
 * through the live coordinator stream to a worker, executed by a real {@link WorkerTaskExecutor},
 * and its results fed back into the coordinator until the job reaches {@code COMPLETED}.
 *
 * <p><b>Worker-message feedback approach:</b> collect-then-drive. The worker-side stream observer
 * only records the inbound {@link TaskAssignment}s; after {@code dispatchOnce} returns, the test
 * thread runs the executor and pumps each produced {@link WorkerMessage} back into the coordinator
 * via the request observer returned by {@code service.taskStream(...)}. Driving the executor outside
 * the coordinator-&gt;worker {@code onNext} callback avoids any StreamObserver re-entrancy against the
 * {@code synchronized(observer)} lock the StreamManager holds while sending an assignment, and keeps
 * the run fully deterministic (no background threads, so no polling needed in practice).
 */
class WorkerExecutionIntegrationTest {

    @Test
    void submittedFilePipelineRunsToCompletion(@TempDir Path tmp) throws Exception {
        // FileSource CSV: first line is the header; the 5 data rows get seqIds 1..5.
        Path input = tmp.resolve("in.csv");
        Files.write(input, String.join("\n",
                "id",
                "1",
                "2",
                "3",
                "4",
                "5").getBytes(StandardCharsets.UTF_8));
        Path output = tmp.resolve("out.csv");

        PipelineConfig cfg = new PipelineConfig(
                "file-to-file",
                new PipelineConfig.SourceConfig("file", input.toString(), "csv"),
                new PipelineConfig.SinkConfig("file", output.toString(), "csv", "overwrite"),
                new PipelineConfig.CheckpointConfig(null));

        // In-process coordinator harness with a SHARED StreamManager (same wiring as DispatchIntegrationTest).
        InMemoryStateStore store = new InMemoryStateStore();
        WorkerRegistry registry = new WorkerRegistry();
        StreamManager streamManager = new StreamManager();

        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                registry, null, new TaskAckHandler(store), store, streamManager);
        TaskDispatcher dispatcher = new TaskDispatcher(
                store,
                new CoordinatorRecoveryPlanner(store),
                new Scheduler(registry, streamManager),
                streamManager,
                30_000L);

        // Worker side: open the coordinator's taskStream. The observer only COLLECTS assignments;
        // the executor is driven from the test thread after dispatch (collect-then-drive).
        List<TaskAssignment> assignments = new ArrayList<>();
        StreamObserver<WorkerMessage> toCoordinator = service.taskStream(new StreamObserver<>() {
            @Override
            public void onNext(CoordinatorMessage value) {
                if (value.hasAssignment()) {
                    assignments.add(value.getAssignment());
                }
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        });

        // Worker registers so the scheduler can pick it.
        toCoordinator.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1").setHost("127.0.0.1").setPort(0).build())
                .build());

        // Submit the real pipeline (enumerates splits + embeds the PipelineConfig) and dispatch.
        new JobSubmissionService(store, new TaskPlanner(),
                new ConnectorFactory(SinkFactoryRegistry.defaultRegistry()))
                .submit("job-1", cfg, 4);
        int dispatched = dispatcher.dispatchOnce(System.currentTimeMillis());

        assertTrue(dispatched > 0, "expected at least one task to be dispatched");
        assertFalse(assignments.isEmpty(), "worker stream should have received assignments");

        // Drive the real worker executor for each delivered assignment, pumping every produced
        // WorkerMessage (CheckpointAcks + final TaskResult) back into the coordinator's request observer.
        WorkerTaskExecutor executor = new WorkerTaskExecutor(
                "worker-1",
                new ConnectorFactory(SinkFactoryRegistry.defaultRegistry()),
                EmbeddingProviderRegistry.defaultRegistry(),
                message -> toCoordinator.onNext(message));
        for (TaskAssignment assignment : assignments) {
            executor.execute(assignment);
        }

        // The entire flow is synchronous: executor.execute() -> toCoordinator.onNext() -> TaskAckHandler
        // all run on the test thread, so the store is already in its final state here.
        JobInstance job = store.getJob("job-1");
        for (String taskId : job.getTaskIds()) {
            assertEquals(TaskState.COMPLETED, store.getTask(taskId).getState(),
                    "task " + taskId + " did not reach COMPLETED");
        }
        assertEquals(TaskState.COMPLETED, store.getJob("job-1").getState(),
                "job-1 did not reach COMPLETED");

        // Each task advanced its checkpoint past the start.
        for (String taskId : job.getTaskIds()) {
            assertTrue(store.getTask(taskId).getLastCheckpointSeq() > 0L,
                    "task " + taskId + " checkpoint did not advance");
        }

        // The output CSV exists and holds the header + 5 data rows.
        assertTrue(Files.exists(output), "output file should exist");
        List<String> lines = Files.readAllLines(output, StandardCharsets.UTF_8);
        assertEquals(6, lines.size(), "expected header + 5 data rows");
        assertEquals("id", lines.get(0));
    }

    /**
     * Resume variant: a task already at {@code lastCheckpointSeq = 2} is re-assigned with
     * {@code startSeq = 3}. The FileSplitReader resumes at {@code max(lastCheckpointSeq, splitStart-1)},
     * so {@code FileSource.readRange} skips seqs &lt;= 2 and only rows 3..5 are read. The overwrite-mode
     * file sink therefore writes exactly the resumed rows (header + rows 3,4,5), proving no
     * re-processing of the checkpointed prefix. We also assert the coordinator drives the task to
     * COMPLETED and the checkpoint advances to the split end.
     */
    @Test
    void resumesFromCheckpointWithoutReprocessing(@TempDir Path tmp) throws Exception {
        Path input = tmp.resolve("in.csv");
        Files.write(input, String.join("\n",
                "id",
                "1",
                "2",
                "3",
                "4",
                "5").getBytes(StandardCharsets.UTF_8));
        Path output = tmp.resolve("out.csv");

        PipelineConfig cfg = new PipelineConfig(
                "file-to-file-resume",
                new PipelineConfig.SourceConfig("file", input.toString(), "csv"),
                new PipelineConfig.SinkConfig("file", output.toString(), "csv", "overwrite"),
                new PipelineConfig.CheckpointConfig(null));

        // Enumerate the real split(s) the same way submission does — a single split [1..5].
        ConnectorFactory factory = new ConnectorFactory(SinkFactoryRegistry.defaultRegistry());
        List<SourceSplit> splits;
        try (SourceEnumerator src = factory.createSource(cfg)) {
            src.open();
            splits = new ArrayList<>(src.enumerateSplits());
        }
        assertEquals(1, splits.size(), "5 rows fit in one default split");

        InMemoryStateStore store = new InMemoryStateStore();

        // Seed a job + a single task already DISPATCHING at lastCheckpointSeq=2, owned by worker-1.
        long leaseUntil = System.currentTimeMillis() + 60_000L;
        TaskDefinition def = new TaskDefinition();
        def.setTaskId("task-1");
        def.setJobName("job-resume");
        Map<String, Object> config = new HashMap<>();
        config.put(JobSubmissionService.SPLITS_CONFIG_KEY, new ArrayList<Object>(splits));
        config.put(WorkerTaskExecutor.PIPELINE_CONFIG_KEY, cfg);
        def.setConfig(config);

        // A recovered attempt: DISPATCHING, owned by worker-1, carrying lastCheckpointSeq=2 and a fresh
        // lease — exactly what the coordinator re-assigns after the first attempt checkpointed seq 2.
        TaskRecord seeded = rebuildAsDispatching(def, "worker-1", "a2", leaseUntil, 2L);

        JobInstance job = new JobInstance();
        job.setJobId("job-resume");
        job.setState(TaskState.CREATED);
        job.setTaskIds(List.of("task-1"));
        store.submitJob(job);
        store.saveTask(seeded);
        assertEquals(2L, store.getTask("task-1").getLastCheckpointSeq());
        assertEquals(TaskState.DISPATCHING, store.getTask("task-1").getState());

        // Coordinator harness to receive the worker results.
        TaskAckHandler ackHandler = new TaskAckHandler(store);

        // Send the assignment with startSeq = lastCheckpointSeq + 1 = 3 (mirrors StreamManager.sendAssignment).
        TaskAssignment assignment = TaskAssignment.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("a2")
                .setDefinition(serialize(def))
                .setStartSeq(3L)
                .setLeaseUntilMillis(leaseUntil)
                .build();

        // Drive the real worker executor; feed CheckpointAcks + TaskResult back into the ack handler.
        // This variant drives the ack handler directly (no live gRPC stream set up here), unlike test 1
        // which wires through the full CoordinatorServiceImpl + StreamManager stream.
        WorkerTaskExecutor executor = new WorkerTaskExecutor(
                "worker-1",
                new ConnectorFactory(SinkFactoryRegistry.defaultRegistry()),
                EmbeddingProviderRegistry.defaultRegistry(),
                message -> {
                    if (message.hasCheckpointAck()) {
                        ackHandler.handleCheckpointAck(message.getCheckpointAck());
                    }
                    if (message.hasTaskResult()) {
                        ackHandler.handleTaskAttemptResult(message.getTaskResult());
                    }
                });
        executor.execute(assignment);

        // Faithful resume assertion: only rows with seq > 2 were read, so the overwrite-mode file sink
        // contains exactly rows 3,4,5 — the checkpointed prefix (rows 1,2) was NOT re-processed.
        assertTrue(Files.exists(output), "output file should exist");
        List<String> lines = Files.readAllLines(output, StandardCharsets.UTF_8);
        assertEquals("id", lines.get(0));
        List<String> dataRows = lines.subList(1, lines.size());
        assertEquals(List.of("3", "4", "5"), dataRows,
                "resume must process only rows after the checkpoint (seq > 2)");

        // The coordinator drove the task (and its job) to COMPLETED and advanced the checkpoint to the split end.
        TaskRecord finalTask = store.getTask("task-1");
        assertEquals(TaskState.COMPLETED, finalTask.getState());
        assertEquals(5L, finalTask.getLastCheckpointSeq(), "checkpoint advances to the split end (seq 5)");
        assertEquals(TaskState.COMPLETED, store.getJob("job-resume").getState());
    }

    /**
     * Materialize a DISPATCHING TaskRecord carrying a non-zero lastCheckpointSeq (a recovered attempt).
     * <p>
     * Reconstructs a DISPATCHING record carrying a prior checkpoint, as it would look after a
     * lease-expiry re-dispatch: CREATED -> DISPATCHING -> RUNNING -> checkpoint -> RETRYING -> DISPATCHING.
     */
    private TaskRecord rebuildAsDispatching(
            TaskDefinition def, String workerId, String attemptId, long leaseUntil, long checkpointSeq) {
        // CREATED -> DISPATCHING -> RUNNING advances the checkpoint, then a re-dispatch would reset to
        // DISPATCHING while preserving the checkpoint. We approximate that recovered state directly.
        TaskRecord running = TaskRecord.created(def)
                .dispatching(workerId, attemptId, leaseUntil)
                .running()
                .checkpoint(attemptId, checkpointSeq);
        // RUNNING -> RETRYING (lease/recovery) -> DISPATCHING preserves lastCheckpointSeq.
        TaskRecord retrying = running.retrying("RECOVER", "recovered");
        return retrying.dispatching(workerId, attemptId, leaseUntil);
    }

    private ByteString serialize(TaskDefinition def) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bytes)) {
            oos.writeObject(def);
        }
        return ByteString.copyFrom(bytes.toByteArray());
    }
}
