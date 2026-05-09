package com.kuaia.engine.pipeline;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;

import java.io.Closeable;
import java.nio.file.Path;

public class LocalPipelineCheckpointStore implements Closeable {
    private static final String LOCAL_WORKER_ID = "local-worker";

    private final RocksDbStateStore stateStore;
    private final String jobId;
    private final String taskId;

    public LocalPipelineCheckpointStore(Path stateDir, String pipelineName) throws Exception {
        this.stateStore = new RocksDbStateStore(stateDir);
        this.jobId = pipelineName;
        this.taskId = taskIdFor(pipelineName);
    }

    public static String taskIdFor(String pipelineName) {
        StringBuilder sanitized = new StringBuilder();
        for (int i = 0; i < pipelineName.length(); i++) {
            char ch = pipelineName.charAt(i);
            if ((ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '.'
                    || ch == '_'
                    || ch == '-') {
                sanitized.append(ch);
            } else {
                sanitized.append('_');
            }
        }
        if (sanitized.length() == 0) {
            sanitized.append("pipeline");
        }
        return "local-pipeline-" + sanitized;
    }

    public TaskRecord startOrResume() throws PipelineExecutionException {
        TaskRecord record = stateStore.getTask(taskId);
        if (record == null) {
            return startNewAttempt(TaskRecord.created(jobId, taskId));
        }
        TaskState state = record.getState();
        if (state == TaskState.CREATED || state == TaskState.RETRYING) {
            return startNewAttempt(record);
        }
        if (state == TaskState.RUNNING || state == TaskState.COMPLETED) {
            return record;
        }
        throw new PipelineExecutionException("Pipeline task " + taskId + " is " + state);
    }

    public TaskRecord checkpoint(TaskRecord record, long processedSeq) {
        TaskRecord updated = record.checkpoint(record.getAttemptId(), processedSeq);
        if (updated != record) {
            stateStore.saveTask(updated);
        }
        return updated;
    }

    public TaskRecord complete(TaskRecord record) {
        if (record.getState() == TaskState.COMPLETED) {
            return record;
        }
        TaskRecord completed = record.complete(record.getAttemptId());
        stateStore.saveTask(completed);
        return completed;
    }

    private TaskRecord startNewAttempt(TaskRecord record) {
        String attemptId = "local-attempt-" + (record.getAttemptNo() + 1);
        TaskRecord running = record
                .dispatching(LOCAL_WORKER_ID, attemptId, Long.MAX_VALUE)
                .running();
        stateStore.saveTask(running);
        return running;
    }

    @Override
    public void close() {
        stateStore.close();
    }
}
