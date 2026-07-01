package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.NodeInfo;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.WorkerRecord;
import com.kuaia.common.rpc.*;
import com.kuaia.engine.coordinator.planner.JobSubmissionService;
import com.kuaia.engine.coordinator.recovery.CoordinatorRecoveryPlanner;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.state.BatchStateFlusher;
import com.kuaia.engine.coordinator.state.StateStore;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineConfigException;
import com.kuaia.engine.pipeline.PipelineConfigLoader;
import io.grpc.stub.StreamObserver;

public class CoordinatorServiceImpl extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {
    private final WorkerRegistry registry;
    private final BatchStateFlusher flusher;
    private final TaskAckHandler ackHandler;
    private final StateStore stateStore;
    private final StreamManager streamManager;
    private final JobSubmissionService submission;

    public CoordinatorServiceImpl(WorkerRegistry registry, BatchStateFlusher flusher) {
        this(registry, flusher, null);
    }

    public CoordinatorServiceImpl(WorkerRegistry registry, BatchStateFlusher flusher, TaskAckHandler ackHandler) {
        this(registry, flusher, ackHandler, null);
    }

    public CoordinatorServiceImpl(
            WorkerRegistry registry,
            BatchStateFlusher flusher,
            TaskAckHandler ackHandler,
            StateStore stateStore) {
        this(registry, flusher, ackHandler, stateStore, new StreamManager());
    }

    /**
     * @param streamManager shared so a {@link com.kuaia.engine.coordinator.dispatch.TaskDispatcher}
     *                      can push assignments to the same live worker streams this service manages.
     */
    public CoordinatorServiceImpl(
            WorkerRegistry registry,
            BatchStateFlusher flusher,
            TaskAckHandler ackHandler,
            StateStore stateStore,
            StreamManager streamManager) {
        this(registry, flusher, ackHandler, stateStore, streamManager, null);
    }

    /**
     * @param submission enumerates a submitted pipeline's splits into persisted CREATED tasks; may be
     *                   {@code null} for harnesses that never call {@link #submitJob}.
     */
    public CoordinatorServiceImpl(
            WorkerRegistry registry,
            BatchStateFlusher flusher,
            TaskAckHandler ackHandler,
            StateStore stateStore,
            StreamManager streamManager,
            JobSubmissionService submission) {
        this.registry = registry;
        this.flusher = flusher;
        this.ackHandler = ackHandler;
        this.stateStore = stateStore;
        this.streamManager = streamManager;
        this.submission = submission;
        if (this.flusher != null) {
            this.flusher.start();
        }
    }

    /** The shared stream manager — pass this to a {@code TaskDispatcher} so both push to the same streams. */
    public StreamManager getStreamManager() {
        return streamManager;
    }

    StreamManager getStreamManagerForTesting() {
        return streamManager;
    }

    @Override
    public StreamObserver<WorkerMessage> taskStream(final StreamObserver<CoordinatorMessage> responseObserver) {
        return new StreamObserver<WorkerMessage>() {
            private String workerId;

            @Override
            public void onNext(WorkerMessage value) {
                if (hasConflictingWorkerIds(value)) {
                    return;
                }
                String resolvedWorkerId = resolveWorkerId(value);
                if (resolvedWorkerId == null || resolvedWorkerId.isEmpty()) {
                    return;
                }
                if (this.workerId != null && !this.workerId.equals(resolvedWorkerId)) {
                    return;
                }
                this.workerId = resolvedWorkerId;
                streamManager.registerStream(workerId, responseObserver);

                if (value.hasHello()) {
                    streamManager.setPaused(workerId, false);
                    registerWorkerHello(value.getHello());
                    replayActiveAssignments(workerId);
                }

                if (value.hasBackpressure()) {
                    boolean paused = value.getBackpressure().getLevel() == BackpressureLevel.BACKPRESSURE_HIGH;
                    streamManager.setPaused(workerId, paused);
                    persistBackpressure(workerId, paused);
                }

                if (value.hasSignal()) {
                    boolean paused = "BACKPRESSURE_HIGH".equals(value.getSignal().getType());
                    streamManager.setPaused(workerId, paused);
                    persistBackpressure(workerId, paused);
                }

                if (value.hasRecordAck() && ackHandler != null) {
                    ackHandler.handleRecordAck(value.getRecordAck());
                }

                if (value.hasCheckpointAck() && ackHandler != null) {
                    ackHandler.handleCheckpointAck(value.getCheckpointAck());
                }

                if (value.hasTaskResult() && ackHandler != null) {
                    ackHandler.handleTaskAttemptResult(value.getTaskResult());
                }

                if (value.hasAck()) {
                    String taskId = value.getAck().getTaskId();
                    if (flusher != null && value.getAck().getSuccess() && taskId != null && !taskId.isEmpty()) {
                        flusher.addAck(taskId);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                persistStreamDisconnected(workerId);
                streamManager.unregisterStream(workerId);
            }

            @Override
            public void onCompleted() {
                persistStreamDisconnected(workerId);
                streamManager.unregisterStream(workerId);
            }
        };
    }

    private boolean hasConflictingWorkerIds(WorkerMessage value) {
        if (value.getWorkerId().isEmpty()) {
            return false;
        }
        String payloadWorkerId = payloadWorkerId(value);
        return payloadWorkerId != null
                && !payloadWorkerId.isEmpty()
                && !value.getWorkerId().equals(payloadWorkerId);
    }

    private String payloadWorkerId(WorkerMessage value) {
        if (value.hasHello()) {
            return value.getHello().getWorkerId();
        }
        if (value.hasHeartbeat()) {
            return value.getHeartbeat().getId();
        }
        if (value.hasRecordAck()) {
            return value.getRecordAck().getWorkerId();
        }
        if (value.hasCheckpointAck()) {
            return value.getCheckpointAck().getWorkerId();
        }
        if (value.hasTaskResult()) {
            return value.getTaskResult().getWorkerId();
        }
        return "";
    }

    private String resolveWorkerId(WorkerMessage value) {
        if (!value.getWorkerId().isEmpty()) {
            return value.getWorkerId();
        }
        return payloadWorkerId(value);
    }

    private void registerWorkerHello(WorkerHello hello) {
        if (hello.getWorkerId().isEmpty()) {
            return;
        }
        NodeInfo node = NodeInfo.builder()
                .id(hello.getWorkerId())
                .host(hello.getHost())
                .port(hello.getPort())
                .type(NodeInfo.NodeType.WORKER)
                .build();
        registry.register(node);
        persistWorker(WorkerRecord.registered(hello.getWorkerId(), hello.getHost(), hello.getPort())
                .withStreamConnected(true));
    }

    private void replayActiveAssignments(String workerId) {
        if (stateStore == null) {
            return;
        }
        long nowMillis = System.currentTimeMillis();
        for (TaskRecord task : stateStore.scanActiveTasksByWorker(workerId)) {
            if (task.getLeaseUntilMillis() <= nowMillis) {
                recoverExpiredAssignment(task);
                continue;
            }
            streamManager.sendAssignment(workerId, task);
        }
    }

    private void recoverExpiredAssignment(TaskRecord task) {
        TaskRecord updated = task.retryingAfterLeaseExpiration(
                CoordinatorRecoveryPlanner.LEASE_EXPIRED,
                "Task lease expired at " + task.getLeaseUntilMillis());
        stateStore.compareAndSetTask(task, updated);
    }

    @Override
    public void registerWorker(WorkerInfo request, StreamObserver<RegistrationResponse> responseObserver) {
        NodeInfo node = NodeInfo.builder()
                .id(request.getId())
                .host(request.getHost())
                .port(request.getPort())
                .type(NodeInfo.NodeType.WORKER)
                .build();
        registry.register(node);
        persistWorker(WorkerRecord.registered(request.getId(), request.getHost(), request.getPort()));
        responseObserver.onNext(RegistrationResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(WorkerHeartbeat request, StreamObserver<HeartbeatResponse> responseObserver) {
        double load = (request.getCpuLoad() + request.getMemLoad()) / 2.0;
        registry.updateHeartbeat(request.getId(), load);
        persistHeartbeat(request.getId(), load);
        responseObserver.onNext(HeartbeatResponse.newBuilder().setAck(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
        if (submission == null) {
            responseObserver.onNext(SubmitJobResponse.newBuilder()
                    .setSuccess(false).setError("submission not configured").build());
            responseObserver.onCompleted();
            return;
        }
        try {
            PipelineConfig cfg = new PipelineConfigLoader().loadFromString(request.getPipelineYaml());
            int maxParallelism = request.getMaxParallelism() <= 0 ? 4 : request.getMaxParallelism();
            String jobId = cfg.getName() + "-" + System.currentTimeMillis();
            JobInstance job = submission.submit(jobId, cfg, maxParallelism);
            int taskCount = job.getTaskIds() == null ? 0 : job.getTaskIds().size();
            responseObserver.onNext(SubmitJobResponse.newBuilder()
                    .setSuccess(true).setJobId(jobId).setTaskCount(taskCount).build());
        } catch (PipelineConfigException e) {
            String msg = e.getMessage() == null ? "invalid pipeline" : e.getMessage();
            responseObserver.onNext(SubmitJobResponse.newBuilder().setSuccess(false).setError(msg).build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getJobStatus(JobStatusRequest request, StreamObserver<JobStatusResponse> responseObserver) {
        JobInstance job = stateStore == null ? null : stateStore.getJob(request.getJobId());
        JobStatusResponse.Builder resp = JobStatusResponse.newBuilder();
        if (job == null) {
            resp.setFound(false);
        } else {
            resp.setFound(true).setJob(toSummary(job));
        }
        responseObserver.onNext(resp.build());
        responseObserver.onCompleted();
    }

    @Override
    public void listJobs(ListJobsRequest request, StreamObserver<ListJobsResponse> responseObserver) {
        ListJobsResponse.Builder resp = ListJobsResponse.newBuilder();
        if (stateStore != null) {
            for (JobInstance job : stateStore.listJobs()) {
                resp.addJobs(toSummary(job));
            }
        }
        responseObserver.onNext(resp.build());
        responseObserver.onCompleted();
    }

    private JobSummary toSummary(JobInstance job) {
        return JobSummary.newBuilder()
                .setJobId(job.getJobId() == null ? "" : job.getJobId())
                .setState(job.getState() == null ? "" : job.getState().name())
                .setTotalTasks(job.getTaskIds() == null ? 0 : job.getTaskIds().size())
                .setCompleted(job.getCompletedTasks())
                .setFailed(job.getFailedTasks())
                .setCancelled(job.getCancelledTasks())
                .build();
    }

    private void persistBackpressure(String workerId, boolean paused) {
        if (stateStore == null || workerId == null || workerId.isEmpty()) {
            return;
        }
        WorkerRecord existing = stateStore.getWorker(workerId);
        if (existing == null) {
            return;
        }
        WorkerRecord.BackpressureLevel level = paused
                ? WorkerRecord.BackpressureLevel.HIGH
                : WorkerRecord.BackpressureLevel.LOW;
        persistWorker(existing.withBackpressure(level));
    }

    private void persistStreamDisconnected(String workerId) {
        if (stateStore == null || workerId == null || workerId.isEmpty()) {
            return;
        }
        WorkerRecord existing = stateStore.getWorker(workerId);
        if (existing != null) {
            persistWorker(existing.withStreamConnected(false));
        }
    }

    private void persistHeartbeat(String workerId, double load) {
        if (stateStore == null || workerId == null || workerId.isEmpty()) {
            return;
        }
        WorkerRecord existing = stateStore.getWorker(workerId);
        if (existing != null) {
            persistWorker(existing.withHeartbeat(load, System.currentTimeMillis()));
        }
    }

    private void persistWorker(WorkerRecord worker) {
        if (stateStore != null && worker != null) {
            stateStore.saveWorker(worker);
        }
    }
}
