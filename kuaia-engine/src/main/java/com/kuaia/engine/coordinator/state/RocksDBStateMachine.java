package com.kuaia.engine.coordinator.state;

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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RocksDBStateMachine extends BaseStateMachine {
    static final String CAS_REJECTED = "CAS_REJECTED";
    private static final String OK = "OK";

    private static final String TASK_PREFIX = "task/";
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
            } else if (cmd.hasTaskRecord()) {
                return applyTaskRecordCommandForTesting(cmd);
            } else if (cmd.hasWorkerRecord()) {
                WorkerRecordPayload payload = cmd.getWorkerRecord();
                WorkerRecord record = deserialize(payload.getRecord().toByteArray(), WorkerRecord.class);
                applyWorkerRecordForTesting(record);
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
                return completedBytes(serialize(scanTaskRecordsByStateForTesting(state)));
            }
            if (key.startsWith(TASK_WORKER_SCAN_PREFIX)) {
                String workerId = key.substring(TASK_WORKER_SCAN_PREFIX.length());
                return completedBytes(serialize(scanActiveTaskRecordsByWorkerForTesting(workerId)));
            }
            if (key.startsWith(WORKER_STATE_SCAN_PREFIX)) {
                WorkerRecord.WorkerState state = WorkerRecord.WorkerState.valueOf(
                        key.substring(WORKER_STATE_SCAN_PREFIX.length()));
                return completedBytes(serialize(scanWorkerRecordsByStateForTesting(state)));
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

    boolean applyTaskRecordForTesting(TaskRecord record, boolean cas, long expectedVersion) throws IOException {
        try {
            TaskRecord current = getTaskRecordForTesting(record.getTaskId());
            if (cas && (current == null || current.getVersion() != expectedVersion)) {
                return false;
            }
            db.put(bytes(taskKey(record.getTaskId())), serialize(record));
            return true;
        } catch (RocksDBException e) {
            throw new IOException("Failed to apply task record " + record.getTaskId(), e);
        }
    }

    CompletableFuture<Message> applyTaskRecordCommandForTesting(RaftCommand command) throws IOException {
        TaskRecordPayload payload = command.getTaskRecord();
        TaskRecord record = deserialize(payload.getRecord().toByteArray(), TaskRecord.class);
        boolean accepted = applyTaskRecordForTesting(
                record,
                command.getType() == CommandType.CAS_TASK_RECORD,
                payload.getExpectedVersion());
        return CompletableFuture.completedFuture(Message.valueOf(accepted ? OK : CAS_REJECTED));
    }

    TaskRecord getTaskRecordForTesting(String taskId) throws IOException {
        try {
            return deserialize(db.get(bytes(taskKey(taskId))), TaskRecord.class);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get task record " + taskId, e);
        }
    }

    void applyWorkerRecordForTesting(WorkerRecord record) throws IOException {
        try {
            db.put(bytes(workerKey(record.getWorkerId())), serialize(record));
        } catch (RocksDBException e) {
            throw new IOException("Failed to apply worker record " + record.getWorkerId(), e);
        }
    }

    WorkerRecord getWorkerRecordForTesting(String workerId) throws IOException {
        try {
            return deserialize(db.get(bytes(workerKey(workerId))), WorkerRecord.class);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get worker record " + workerId, e);
        }
    }

    List<TaskRecord> scanTaskRecordsByStateForTesting(TaskState state) throws IOException {
        return scanTaskRecords().stream()
                .filter(record -> record.getState() == state)
                .sorted(Comparator.comparing(TaskRecord::getTaskId))
                .collect(Collectors.toList());
    }

    List<TaskRecord> scanActiveTaskRecordsByWorkerForTesting(String workerId) throws IOException {
        return scanTaskRecords().stream()
                .filter(record -> workerId.equals(record.getAssignedWorkerId()))
                .filter(record -> isActive(record.getState()))
                .sorted(Comparator.comparing(TaskRecord::getTaskId))
                .collect(Collectors.toList());
    }

    List<WorkerRecord> scanWorkerRecordsByStateForTesting(WorkerRecord.WorkerState state) throws IOException {
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

    @Override
    public void close() throws IOException {
        super.close();
        if (db != null) db.close();
    }
}
