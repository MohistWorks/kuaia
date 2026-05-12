package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import com.kuaia.common.raft.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import java.io.*;
import java.util.*;

public class RatisStateStore implements StateStore {
    private static final String TASK_PREFIX = "task/";
    private static final String WORKER_PREFIX = "worker/";
    private static final String TASK_STATE_SCAN_PREFIX = "scan/task_state/";
    private static final String TASK_WORKER_SCAN_PREFIX = "scan/task_worker/";
    private static final String WORKER_STATE_SCAN_PREFIX = "scan/worker_state/";

    private final RaftClient raftClient;

    public RatisStateStore(RaftClient raftClient) {
        this.raftClient = raftClient;
    }

    @Override
    public void saveTask(TaskRecord record) {
        try {
            RaftCommand cmd = RaftCommand.newBuilder()
                    .setType(CommandType.SAVE_TASK_RECORD)
                    .setTaskRecord(TaskRecordPayload.newBuilder()
                            .setTaskId(record.getTaskId())
                            .setRecord(com.google.protobuf.ByteString.copyFrom(serialize(record)))
                            .build())
                    .build();
            sendWrite(cmd, "save task record " + record.getTaskId());
        } catch (IOException e) {
            throw new RuntimeException("Failed to save task record via Raft", e);
        }
    }

    @Override
    public TaskRecord getTask(String taskId) {
        try {
            RaftClientReply reply = raftClient.io().sendReadOnly(Message.valueOf(TASK_PREFIX + taskId));
            if (!reply.isSuccess()) {
                throw new IOException("Raft read failed: " + reply.getException());
            }
            byte[] data = reply.getMessage().getContent().toByteArray();
            if (data.length == 0) {
                return null;
            }
            return deserialize(data, TaskRecord.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get task record via Raft", e);
        }
    }

    @Override
    public boolean compareAndSetTask(TaskRecord expected, TaskRecord updated) {
        if (!expected.getTaskId().equals(updated.getTaskId())) {
            return false;
        }
        try {
            RaftCommand cmd = RaftCommand.newBuilder()
                    .setType(CommandType.CAS_TASK_RECORD)
                    .setTaskRecord(TaskRecordPayload.newBuilder()
                            .setTaskId(updated.getTaskId())
                            .setRecord(com.google.protobuf.ByteString.copyFrom(serialize(updated)))
                            .setExpectedVersion(expected.getVersion())
                            .build())
                    .build();
            RaftClientReply reply = sendWrite(cmd, "compare-and-set task record " + updated.getTaskId());
            return !isCasRejected(reply);
        } catch (IOException e) {
            throw new RuntimeException("Failed to compare-and-set task record via Raft", e);
        }
    }

    @Override
    public List<TaskRecord> scanTasksByState(TaskState state) {
        return readList(TASK_STATE_SCAN_PREFIX + state.name(), TaskRecord.class);
    }

    @Override
    public List<TaskRecord> scanActiveTasksByWorker(String workerId) {
        return readList(TASK_WORKER_SCAN_PREFIX + workerId, TaskRecord.class);
    }

    @Override
    public void saveWorker(WorkerRecord record) {
        try {
            RaftCommand cmd = RaftCommand.newBuilder()
                    .setType(CommandType.SAVE_WORKER_RECORD)
                    .setWorkerRecord(WorkerRecordPayload.newBuilder()
                            .setWorkerId(record.getWorkerId())
                            .setRecord(com.google.protobuf.ByteString.copyFrom(serialize(record)))
                            .build())
                    .build();
            sendWrite(cmd, "save worker record " + record.getWorkerId());
        } catch (IOException e) {
            throw new RuntimeException("Failed to save worker record via Raft", e);
        }
    }

    @Override
    public WorkerRecord getWorker(String workerId) {
        try {
            RaftClientReply reply = raftClient.io().sendReadOnly(Message.valueOf(WORKER_PREFIX + workerId));
            if (!reply.isSuccess()) {
                throw new IOException("Raft read failed: " + reply.getException());
            }
            byte[] data = reply.getMessage().getContent().toByteArray();
            if (data.length == 0) {
                return null;
            }
            return deserialize(data, WorkerRecord.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get worker record via Raft", e);
        }
    }

    @Override
    public List<WorkerRecord> scanWorkersByState(WorkerRecord.WorkerState state) {
        return readList(WORKER_STATE_SCAN_PREFIX + state.name(), WorkerRecord.class);
    }

    @Override
    @Deprecated
    public void saveTask(TaskDefinition task, TaskState state) {
        saveTask(TaskRecord.fromLegacyState(task, state));
    }

    public void updateTaskState(String taskId, TaskState state) {
        TaskRecord record = getTask(taskId);
        if (record != null) {
            if (!compareAndSetTask(record, record.withLegacyState(state))) {
                throw new RuntimeException("Task " + taskId + " changed while updating state via Raft");
            }
            return;
        }
        try {
            RaftCommand cmd = RaftCommand.newBuilder()
                    .setType(CommandType.UPDATE_STATE)
                    .setUpdateState(UpdateStatePayload.newBuilder()
                            .setTaskId(taskId)
                            .setStateCode(state.ordinal())
                            .build())
                    .build();

            sendWrite(cmd, "update task state " + taskId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update state via Raft", e);
        }
    }

    private RaftClientReply sendWrite(RaftCommand command, String description) throws IOException {
        RaftClientReply reply = raftClient.io().send(Message.valueOf(
                org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(command.toByteArray())));
        if (!reply.isSuccess()) {
            throw new IOException("Raft write failed for " + description + ": " + reply.getException());
        }
        return reply;
    }

    private boolean isCasRejected(RaftClientReply reply) {
        Message message = reply.getMessage();
        if (message == null) {
            return false;
        }
        return RocksDBStateMachine.CAS_REJECTED.equals(message.getContent().toStringUtf8());
    }

    private byte[] serialize(Object value) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
        objectStream.writeObject(value);
        objectStream.flush();
        return byteStream.toByteArray();
    }

    private <T> T deserialize(byte[] bytes, Class<T> type) throws IOException {
        try {
            ObjectInputStream objectStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return type.cast(objectStream.readObject());
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize " + type.getSimpleName(), e);
        }
    }

    private <T> List<T> readList(String queryKey, Class<T> elementType) {
        try {
            RaftClientReply reply = raftClient.io().sendReadOnly(Message.valueOf(queryKey));
            if (!reply.isSuccess()) {
                throw new IOException("Raft read failed: " + reply.getException());
            }
            byte[] data = reply.getMessage().getContent().toByteArray();
            if (data.length == 0) {
                return Collections.emptyList();
            }
            List<?> values = deserialize(data, List.class);
            List<T> result = new ArrayList<>();
            for (Object value : values) {
                result.add(elementType.cast(value));
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan Raft state for " + queryKey, e);
        }
    }

    @Override
    @Deprecated
    public TaskState getTaskState(String taskId) {
        TaskRecord record = getTask(taskId);
        if (record != null) {
            return record.getState();
        }
        return getLegacyTaskState(taskId);
    }

    private TaskState getLegacyTaskState(String taskId) {
        try {
            // Linearizable read via query
            RaftClientReply reply = raftClient.io().sendReadOnly(Message.valueOf(taskId + "_state"));
            if (!reply.isSuccess()) {
                throw new IOException("Raft read failed: " + reply.getException());
            }
            byte[] data = reply.getMessage().getContent().toByteArray();
            if (data.length == 0) return null;
            int stateCode = Integer.parseInt(new String(data));
            return TaskState.values()[stateCode];
        } catch (IOException e) {
            throw new RuntimeException("Failed to get state via Raft", e);
        }
    }

    @Override
    @Deprecated
    public List<TaskDefinition> getTasksByState(TaskState state) {
        return StateStore.super.getTasksByState(state);
    }
}
