package com.kuaia.engine.coordinator.state;

import com.google.protobuf.ByteString;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.raft.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class RocksDBStateMachine extends BaseStateMachine {
    private static final String TASK_PREFIX = "task/";

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
                TaskRecordPayload payload = cmd.getTaskRecord();
                TaskRecord record = deserialize(payload.getRecord().toByteArray(), TaskRecord.class);
                boolean accepted = applyTaskRecordForTesting(
                        record,
                        cmd.getType() == CommandType.CAS_TASK_RECORD,
                        payload.getExpectedVersion());
                if (!accepted) {
                    throw new IOException("Rejected stale task record command for " + payload.getTaskId());
                }
            }
        } catch (Exception e) {
            return failedFuture(e);
        }
        return CompletableFuture.completedFuture(Message.valueOf("OK"));
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        String key = request.getContent().toStringUtf8();
        try {
            byte[] val = db.get(bytes(key));
            if (val == null) {
                return CompletableFuture.completedFuture(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.EMPTY));
            }
            return CompletableFuture.completedFuture(Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(val)));
        } catch (Exception e) {
            return failedFuture(e);
        }
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

    TaskRecord getTaskRecordForTesting(String taskId) throws IOException {
        try {
            return deserialize(db.get(bytes(taskKey(taskId))), TaskRecord.class);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get task record " + taskId, e);
        }
    }

    private String taskKey(String taskId) {
        return TASK_PREFIX + taskId;
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
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
