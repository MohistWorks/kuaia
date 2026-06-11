package com.kuaia.engine.worker;

import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.rpc.AttemptStatus;
import com.kuaia.common.rpc.CheckpointAck;
import com.kuaia.common.rpc.TaskAssignment;
import com.kuaia.common.rpc.TaskAttemptResult;
import com.kuaia.common.rpc.WorkerMessage;
import com.kuaia.engine.coordinator.planner.JobSubmissionService;
import com.kuaia.engine.pipeline.ConnectorFactory;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.SplitExecutor;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import com.kuaia.engine.pipeline.transform.TransformPipeline;
import com.kuaia.engine.worker.connector.v2.BatchSinkWriter;
import com.kuaia.engine.worker.connector.v2.SourceEnumerator;
import com.kuaia.engine.worker.connector.v2.SourceSplit;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a single task bundle on a worker: deserializes the assignment's {@link TaskDefinition},
 * assembles the read/transform/write pipeline, runs each source split (in seq order, skipping splits
 * already fully below the resume point), emits a {@link CheckpointAck} per committed batch, and
 * finishes with a single {@link TaskAttemptResult}.
 *
 * <p>{@link #execute} is synchronous and never throws: every outcome — including assembly and
 * execution failures — is reported through the injected {@link WorkerMessageSink}. Callers run this
 * on a worker thread.
 */
public class WorkerTaskExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerTaskExecutor.class);

    /** Config key under which a task definition carries its serialized {@link PipelineConfig}. */
    public static final String PIPELINE_CONFIG_KEY = "pipeline";

    /** Permanent failure: the assignment could not be assembled into a runnable pipeline. */
    private static final String ERROR_CODE_ASSEMBLY = "ASSEMBLY";

    /** Retryable failure: the pipeline assembled but execution failed partway through. */
    private static final String ERROR_CODE_TRANSIENT = "TRANSIENT";

    /** Sink that swallows connector diagnostics so they never leak to the worker's stdout. */
    private static final PrintStream DISCARD = new PrintStream(OutputStream.nullOutputStream());

    private final String workerId;
    private final ConnectorFactory connectorFactory;
    private final EmbeddingProviderRegistry embeddingProviders;
    private final WorkerMessageSink sink;
    private final SplitExecutor splitExecutor = new SplitExecutor();

    public WorkerTaskExecutor(
            String workerId,
            ConnectorFactory connectorFactory,
            EmbeddingProviderRegistry embeddingProviders,
            WorkerMessageSink sink) {
        this.workerId = workerId;
        this.connectorFactory = connectorFactory;
        this.embeddingProviders = embeddingProviders;
        this.sink = sink;
    }

    public void execute(TaskAssignment assignment) {
        String taskId = assignment.getTaskId();
        String attemptId = assignment.getAttemptId();
        try {
            runTaskBundle(assignment, taskId, attemptId);
            send(successMessage(taskId, attemptId));
        } catch (AssemblyException e) {
            LOG.warn("Task {} attempt {} failed during assembly", taskId, attemptId, e);
            send(failureMessage(taskId, attemptId, ERROR_CODE_ASSEMBLY, e.getMessage()));
        } catch (Exception e) {
            LOG.warn("Task {} attempt {} failed during execution", taskId, attemptId, e);
            send(failureMessage(taskId, attemptId, ERROR_CODE_TRANSIENT, e.getMessage()));
        }
    }

    private void runTaskBundle(TaskAssignment assignment, String taskId, String attemptId) throws Exception {
        TaskDefinition def = deserialize(assignment);
        Map<String, Object> config = def.getConfig();
        if (config == null || !(config.get(PIPELINE_CONFIG_KEY) instanceof PipelineConfig)) {
            throw new AssemblyException("Task definition is missing a '" + PIPELINE_CONFIG_KEY + "' PipelineConfig");
        }
        PipelineConfig pipelineConfig = (PipelineConfig) config.get(PIPELINE_CONFIG_KEY);
        List<SourceSplit> splits = extractSplits(config);

        long lastCheckpointSeq = assignment.getStartSeq() - 1;

        SourceEnumerator source = null;
        BatchSinkWriter writer = null;
        try {
            source = assemble(() -> {
                SourceEnumerator s = connectorFactory.createSource(pipelineConfig);
                s.open();
                return s;
            });
            final SourceEnumerator openedSource = source;
            TransformPipeline transforms = assemble(() ->
                    TransformPipeline.from(openedSource.getRowType(), pipelineConfig.getTransforms(), embeddingProviders));
            writer = assemble(() -> {
                BatchSinkWriter w = connectorFactory.createSink(pipelineConfig, transforms.getOutputType(), DISCARD);
                w.open();
                return w;
            });

            List<SourceSplit> ordered = new ArrayList<>(splits);
            ordered.sort(Comparator.comparingLong(SourceSplit::getStartSeqInclusive));
            for (SourceSplit split : ordered) {
                if (split.getEndSeqInclusive() <= lastCheckpointSeq) {
                    continue;
                }
                splitExecutor.execute(
                        openedSource,
                        transforms,
                        writer,
                        split,
                        lastCheckpointSeq,
                        pipelineConfig.getErrorPolicy(),
                        DISCARD,
                        maxSeq -> send(checkpointMessage(taskId, attemptId, maxSeq)));
            }
        } finally {
            closeQuietly(source);
            closeQuietly(writer);
        }
    }

    private TaskDefinition deserialize(TaskAssignment assignment) throws AssemblyException {
        try (ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(assignment.getDefinition().toByteArray()))) {
            Object value = in.readObject();
            if (!(value instanceof TaskDefinition)) {
                throw new AssemblyException("Assignment definition is not a TaskDefinition");
            }
            return (TaskDefinition) value;
        } catch (AssemblyException e) {
            throw e;
        } catch (Exception e) {
            throw new AssemblyException("Failed to deserialize task definition: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<SourceSplit> extractSplits(Map<String, Object> config) throws AssemblyException {
        Object raw = config.get(JobSubmissionService.SPLITS_CONFIG_KEY);
        if (!(raw instanceof List<?>)) {
            throw new AssemblyException("Task definition is missing a '"
                    + JobSubmissionService.SPLITS_CONFIG_KEY + "' split list");
        }
        List<SourceSplit> splits = new ArrayList<>();
        for (Object element : (List<Object>) raw) {
            if (!(element instanceof SourceSplit)) {
                throw new AssemblyException("Split list contains a non-SourceSplit element: " + element);
            }
            splits.add((SourceSplit) element);
        }
        return splits;
    }

    private <T> T assemble(AssemblyStep<T> step) throws AssemblyException {
        try {
            return step.run();
        } catch (Exception e) {
            throw new AssemblyException(e.getMessage(), e);
        }
    }

    private void send(WorkerMessage message) {
        sink.send(message);
    }

    private WorkerMessage checkpointMessage(String taskId, String attemptId, long processedSeq) {
        return WorkerMessage.newBuilder()
                .setWorkerId(workerId)
                .setCheckpointAck(CheckpointAck.newBuilder()
                        .setTaskId(taskId)
                        .setAttemptId(attemptId)
                        .setWorkerId(workerId)
                        .setProcessedSeq(processedSeq)
                        .build())
                .build();
    }

    private WorkerMessage successMessage(String taskId, String attemptId) {
        return WorkerMessage.newBuilder()
                .setWorkerId(workerId)
                .setTaskResult(TaskAttemptResult.newBuilder()
                        .setTaskId(taskId)
                        .setAttemptId(attemptId)
                        .setWorkerId(workerId)
                        .setStatus(AttemptStatus.ATTEMPT_SUCCESS)
                        .build())
                .build();
    }

    private WorkerMessage failureMessage(String taskId, String attemptId, String errorCode, String errorMessage) {
        return WorkerMessage.newBuilder()
                .setWorkerId(workerId)
                .setTaskResult(TaskAttemptResult.newBuilder()
                        .setTaskId(taskId)
                        .setAttemptId(attemptId)
                        .setWorkerId(workerId)
                        .setStatus(AttemptStatus.ATTEMPT_FAILED)
                        .setErrorCode(errorCode)
                        .setErrorMessage(errorMessage == null ? "" : errorMessage)
                        .build())
                .build();
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.warn("Failed to close {}", closeable.getClass().getSimpleName(), e);
        }
    }

    /** Outbound channel for worker messages; production wiring uses {@code WorkerNode.sendMessage}. */
    public interface WorkerMessageSink {
        void send(WorkerMessage message);
    }

    private interface AssemblyStep<T> {
        T run() throws Exception;
    }

    /** Marks a permanent assembly-phase failure, distinct from a retryable execution-phase failure. */
    private static final class AssemblyException extends Exception {
        private static final long serialVersionUID = 1L;

        AssemblyException(String message) {
            super(message);
        }

        AssemblyException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
