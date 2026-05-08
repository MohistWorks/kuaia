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
            sendWrite(cmd, "compare-and-set task record " + updated.getTaskId());
            return true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to compare-and-set task record via Raft", e);
        }
    }

    @Override
    public List<TaskRecord> scanTasksByState(TaskState state) {
        return Collections.emptyList();
    }

    @Override
    public List<TaskRecord> scanActiveTasksByWorker(String workerId) {
        return Collections.emptyList();
    }

    @Override
    public void saveWorker(WorkerRecord record) {
        throw new UnsupportedOperationException("Worker records require the Phase 4 Raft command model");
    }

    @Override
    public WorkerRecord getWorker(String workerId) {
        return null;
    }

    @Override
    public List<WorkerRecord> scanWorkersByState(WorkerRecord.WorkerState state) {
        return Collections.emptyList();
    }

    public void saveTask(TaskDefinition task, TaskState state) {
        try {
            // Serialize TaskDefinition to bytes (using standard Java serialization for now as it implements Serializable)
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(task);
            oos.flush();

            RaftCommand cmd = RaftCommand.newBuilder()
                    .setType(CommandType.SAVE_TASK)
                    .setSaveTask(SaveTaskPayload.newBuilder()
                            .setTaskId(task.getTaskId())
                            .setDefinition(com.google.protobuf.ByteString.copyFrom(bos.toByteArray()))
                            .build())
                    .build();

            sendWrite(cmd, "save task " + task.getTaskId());

            // Also update state
            updateTaskState(task.getTaskId(), state);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save task via Raft", e);
        }
    }

    public void updateTaskState(String taskId, TaskState state) {
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

    private void sendWrite(RaftCommand command, String description) throws IOException {
        RaftClientReply reply = raftClient.io().send(Message.valueOf(
                org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(command.toByteArray())));
        if (!reply.isSuccess()) {
            throw new IOException("Raft write failed for " + description + ": " + reply.getException());
        }
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

    public TaskState getTaskState(String taskId) {
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

    public List<TaskDefinition> getTasksByState(TaskState state) {
        // This is a complex query for Raft/RocksDB.
        // For MVP, we can return an empty list or implement a scan.
        // Let's return empty for now as defined in the simplified skeleton.
        return Collections.emptyList();
    }
}
