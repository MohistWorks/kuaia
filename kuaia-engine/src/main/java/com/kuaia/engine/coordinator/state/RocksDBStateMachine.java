package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.JobStateEvaluator;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import com.kuaia.common.raft.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RocksDBStateMachine extends BaseStateMachine {
    static final String CAS_REJECTED = "CAS_REJECTED";
    private static final String OK = "OK";

    private static final String TASK_PREFIX = "task/";
    private static final String JOB_PREFIX = "job/";
    private static final String WORKER_PREFIX = "worker/";
    private static final String TASK_STATE_SCAN_PREFIX = "scan/task_state/";
    private static final String TASK_WORKER_SCAN_PREFIX = "scan/task_worker/";
    private static final String WORKER_STATE_SCAN_PREFIX = "scan/worker_state/";

    private RocksDB db;

    public void initialize(String path) throws IOException {
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        try {
            this.db = RocksDB.open(options, path);
        } catch (org.rocksdb.RocksDBException e) {
            throw new IOException(e);
        }
    }

    private <T> CompletableFuture<T> failedFuture(Throwable e) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(e);
        return future;
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        byte[] data = trx.getLogEntry().getStateMachineLogEntry().getLogData().toByteArray();
        try {
            RaftCommand cmd = RaftCommand.parseFrom(data);
            if (cmd.hasSaveTask()) {
                SaveTaskPayload payload = cmd.getSaveTask();
                db.put(payload.getTaskId().getBytes(), payload.getDefinition().toByteArray());
            } else if (cmd.hasUpdateState()) {
                UpdateStatePayload payload = cmd.getUpdateState();
                db.put((payload.getTaskId() + "_state").getBytes(),
                       String.valueOf(payload.getStateCode()).getBytes());
                syncTaskRecordState(payload.getTaskId(), payload.getStateCode());
                evaluateJobState(payload.getTaskId());
            } else if (cmd.hasSubmitJob()) {
                SubmitJobPayload payload = cmd.getSubmitJob();
                applySubmitJob(payload);
            } else if (cmd.hasUpdateJobState()) {
                UpdateJobStatePayload payload = cmd.getUpdateJobState();
                applyUpdateJobState(payload);
            } else if (cmd.hasTaskRecord()) {
                return applyTaskRecordCommand(cmd);
            } else if (cmd.hasWorkerRecord()) {
                WorkerRecordPayload payload = cmd.getWorkerRecord();
                WorkerRecord record = deserialize(payload.getRecord().toByteArray(), WorkerRecord.class);
                applyWorkerRecord(record);
            }
        } catch (Exception e) {
            return failedFuture(e);
        }
        return CompletableFuture.completedFuture(Message.valueOf(OK));
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        String key = request.getContent().toStringUtf8();
        try {
            if (key.startsWith(TASK_STATE_SCAN_PREFIX)) {
                TaskState state = TaskState.valueOf(key.substring(TASK_STATE_SCAN_PREFIX.length()));
                return completedBytes(serialize(scanTaskRecordsByState(state)));
            }
            if (key.startsWith(TASK_WORKER_SCAN_PREFIX)) {
                String workerId = key.substring(TASK_WORKER_SCAN_PREFIX.length());
                return completedBytes(serialize(scanActiveTaskRecordsByWorker(workerId)));
            }
            if (key.startsWith(WORKER_STATE_SCAN_PREFIX)) {
                WorkerRecord.WorkerState state = WorkerRecord.WorkerState.valueOf(
                        key.substring(WORKER_STATE_SCAN_PREFIX.length()));
                return completedBytes(serialize(scanWorkerRecordsByState(state)));
            }
            byte[] val = db.get(bytes(key));
            if (val == null) {
                return CompletableFuture.completedFuture(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.EMPTY));
            }
            return CompletableFuture.completedFuture(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(val)));
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    private CompletableFuture<Message> completedBytes(byte[] value) {
        return CompletableFuture.completedFuture(Message.valueOf(
                org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(value)));
    }

    boolean applyTaskRecord(TaskRecord record, boolean cas, long expectedVersion) throws IOException {
        try {
            TaskRecord current = getTaskRecord(record.getTaskId());
            if (cas && (current == null || current.getVersion() != expectedVersion)) {
                return false;
            }
            db.put(bytes(taskKey(record.getTaskId())), serialize(record));
            // Production task-completion path: when a task reaches a terminal state, atomically
            // re-evaluate the parent job (design §4 — runs inside a serial Raft apply).
            if (!JobStateEvaluator.isActive(record.getState())) {
                evaluateJobState(record.getTaskId());
            }
            return true;
        } catch (RocksDBException e) {
            throw new IOException("Failed to apply task record " + record.getTaskId(), e);
        }
    }

    CompletableFuture<Message> applyTaskRecordCommand(RaftCommand command) throws IOException {
        TaskRecordPayload payload = command.getTaskRecord();
        TaskRecord record = deserialize(payload.getRecord().toByteArray(), TaskRecord.class);
        boolean accepted = applyTaskRecord(
                record,
                command.getType() == CommandType.CAS_TASK_RECORD,
                payload.getExpectedVersion());
        return CompletableFuture.completedFuture(Message.valueOf(accepted ? OK : CAS_REJECTED));
    }

    TaskRecord getTaskRecord(String taskId) throws IOException {
        try {
            return deserialize(db.get(bytes(taskKey(taskId))), TaskRecord.class);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get task record " + taskId, e);
        }
    }

    void applyWorkerRecord(WorkerRecord record) throws IOException {
        try {
            db.put(bytes(workerKey(record.getWorkerId())), serialize(record));
        } catch (RocksDBException e) {
            throw new IOException("Failed to apply worker record " + record.getWorkerId(), e);
        }
    }

    WorkerRecord getWorkerRecord(String workerId) throws IOException {
        try {
            return deserialize(db.get(bytes(workerKey(workerId))), WorkerRecord.class);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get worker record " + workerId, e);
        }
    }

    List<TaskRecord> scanTaskRecordsByState(TaskState state) throws IOException {
        return scanTaskRecords().stream()
                .filter(record -> record.getState() == state)
                .sorted(Comparator.comparing(TaskRecord::getTaskId))
                .collect(Collectors.toList());
    }

    List<TaskRecord> scanActiveTaskRecordsByWorker(String workerId) throws IOException {
        return scanTaskRecords().stream()
                .filter(record -> workerId.equals(record.getAssignedWorkerId()))
                .filter(record -> isActive(record.getState()))
                .sorted(Comparator.comparing(TaskRecord::getTaskId))
                .collect(Collectors.toList());
    }

    List<WorkerRecord> scanWorkerRecordsByState(WorkerRecord.WorkerState state) throws IOException {
        return scanWorkerRecords().stream()
                .filter(record -> record.getState() == state)
                .sorted(Comparator.comparing(WorkerRecord::getWorkerId))
                .collect(Collectors.toList());
    }

    private List<TaskRecord> scanTaskRecords() throws IOException {
        return scanRecords(TASK_PREFIX, TaskRecord.class);
    }

    private List<WorkerRecord> scanWorkerRecords() throws IOException {
        return scanRecords(WORKER_PREFIX, WorkerRecord.class);
    }

    private <T> List<T> scanRecords(String prefix, Class<T> type) throws IOException {
        List<T> records = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator()) {
            byte[] prefixBytes = bytes(prefix);
            for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next()) {
                String key = string(iterator.key());
                if (!key.startsWith(prefix)) {
                    break;
                }
                records.add(deserialize(iterator.value(), type));
            }
            iterator.status();
            return records;
        } catch (RocksDBException e) {
            throw new IOException("Failed to scan prefix " + prefix, e);
        }
    }

    private boolean isActive(TaskState state) {
        return state == TaskState.DISPATCHING || state == TaskState.RUNNING;
    }

    private String taskKey(String taskId) {
        return TASK_PREFIX + taskId;
    }

    private String workerKey(String workerId) {
        return WORKER_PREFIX + workerId;
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private String string(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private byte[] serialize(Object value) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
        objectStream.writeObject(value);
        objectStream.flush();
        return byteStream.toByteArray();
    }

    private <T> T deserialize(byte[] bytes, Class<T> type) throws IOException {
        if (bytes == null) {
            return null;
        }
        try {
            ObjectInputStream objectStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return type.cast(objectStream.readObject());
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize " + type.getSimpleName(), e);
        }
    }

    void applySubmitJob(SubmitJobPayload payload) throws IOException {
        try {
            db.put(bytes(jobKey(payload.getJobId())), payload.getDefinition().toByteArray());
        } catch (RocksDBException e) {
            throw new IOException("Failed to submit job " + payload.getJobId(), e);
        }
    }

    void applyUpdateJobState(UpdateJobStatePayload payload) throws IOException {
        JobInstance job = getJobInstance(payload.getJobId());
        if (job == null) {
            return;
        }
        job.setState(TaskState.values()[payload.getStateCode()]);
        try {
            db.put(bytes(jobKey(payload.getJobId())), serialize(job));
        } catch (RocksDBException e) {
            throw new IOException("Failed to update job state " + payload.getJobId(), e);
        }
    }

    JobInstance getJobInstance(String jobId) throws IOException {
        try {
            return deserialize(db.get(bytes(jobKey(jobId))), JobInstance.class);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get job instance " + jobId, e);
        }
    }

    void syncTaskRecordState(String taskId, int stateCode) throws IOException {
        TaskRecord current = getTaskRecord(taskId);
        if (current == null) {
            return;
        }
        TaskState newState = TaskState.values()[stateCode];
        if (current.getState() == newState) {
            return;
        }
        TaskRecord updated = current.withLegacyState(newState);
        try {
            db.put(bytes(taskKey(taskId)), serialize(updated));
        } catch (RocksDBException e) {
            throw new IOException("Failed to sync task record state " + taskId, e);
        }
    }

    void evaluateJobState(String taskId) throws IOException {
        TaskRecord taskRecord = getTaskRecord(taskId);
        if (taskRecord == null) {
            return;
        }
        // Only a terminal task can finalize its job; an active task triggers no re-evaluation.
        if (JobStateEvaluator.isActive(taskRecord.getState())) {
            return;
        }

        try {
            JobInstance job = getJobInstance(taskRecord.getJobId());
            if (job == null || job.getTaskIds() == null) {
                return;
            }

            List<TaskState> childStates = new ArrayList<>();
            for (String childTaskId : job.getTaskIds()) {
                TaskRecord child = getTaskRecord(childTaskId);
                childStates.add(child == null ? null : child.getState());
            }

            Optional<TaskState> decided = JobStateEvaluator.evaluate(childStates);
            if (decided.isEmpty() || decided.get() == job.getState()) {
                return;
            }
            job.setState(decided.get());
            db.put(bytes(jobKey(job.getJobId())), serialize(job));
        } catch (RocksDBException e) {
            throw new IOException("Failed to evaluate job state for task " + taskId, e);
        }
    }

    private String jobKey(String jobId) {
        return JOB_PREFIX + jobId;
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (db != null) db.close();
    }
}
