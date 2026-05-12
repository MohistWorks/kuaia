package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import com.kuaia.common.raft.CommandType;
import com.kuaia.common.raft.RaftCommand;
import com.kuaia.common.raft.TaskRecordPayload;
import org.apache.ratis.protocol.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RocksDBStateMachineTest {
    @TempDir
    Path tempDir;

    @Test
    void appliesTaskRecordCommandsAndRejectsStaleCas() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());
        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();

        assertTrue(stateMachine.applyTaskRecordForTesting(running, false, -1L));
        TaskRecord checkpointed = running.checkpoint("attempt-1", 10L);
        assertTrue(stateMachine.applyTaskRecordForTesting(checkpointed, true, running.getVersion()));
        assertFalse(stateMachine.applyTaskRecordForTesting(running.complete("attempt-1"), true, running.getVersion()));

        TaskRecord stored = stateMachine.getTaskRecordForTesting("task-1");
        assertEquals(TaskState.RUNNING, stored.getState());
        assertEquals(10L, stored.getLastCheckpointSeq());
        stateMachine.close();
    }

    @Test
    void staleCasCommandReturnsRejectedMessageWithoutOverwritingRecord() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());
        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();
        TaskRecord checkpointed = running.checkpoint("attempt-1", 10L);

        assertTrue(stateMachine.applyTaskRecordForTesting(running, false, -1L));
        assertTrue(stateMachine.applyTaskRecordForTesting(checkpointed, true, running.getVersion()));

        Message result = stateMachine
                .applyTaskRecordCommandForTesting(casCommand(running.complete("attempt-1"), running.getVersion()))
                .get();

        assertEquals(RocksDBStateMachine.CAS_REJECTED, result.getContent().toStringUtf8());
        TaskRecord stored = stateMachine.getTaskRecordForTesting("task-1");
        assertEquals(TaskState.RUNNING, stored.getState());
        assertEquals(10L, stored.getLastCheckpointSeq());
        stateMachine.close();
    }

    @Test
    void scansTaskAndWorkerRecordsForStateStoreContract() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());
        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();
        TaskRecord completed = TaskRecord.created("job-1", "task-2")
                .dispatching("worker-1", "attempt-2", 10_000L)
                .running()
                .complete("attempt-2");

        assertTrue(stateMachine.applyTaskRecordForTesting(running, false, -1L));
        assertTrue(stateMachine.applyTaskRecordForTesting(completed, false, -1L));
        stateMachine.applyWorkerRecordForTesting(WorkerRecord.registered("worker-1", "127.0.0.1", 9001));
        stateMachine.applyWorkerRecordForTesting(WorkerRecord.registered("worker-2", "127.0.0.1", 9002)
                .withState(WorkerRecord.WorkerState.OFFLINE));

        assertEquals(1, stateMachine.scanTaskRecordsByStateForTesting(TaskState.RUNNING).size());
        assertEquals(1, stateMachine.scanActiveTaskRecordsByWorkerForTesting("worker-1").size());
        assertEquals("worker-1", stateMachine.getWorkerRecordForTesting("worker-1").getWorkerId());
        assertEquals(1, stateMachine.scanWorkerRecordsByStateForTesting(WorkerRecord.WorkerState.REGISTERED).size());

        List<TaskRecord> queriedTasks = deserializeList(
                stateMachine.query(Message.valueOf("scan/task_state/RUNNING")).get(),
                TaskRecord.class);
        List<WorkerRecord> queriedWorkers = deserializeList(
                stateMachine.query(Message.valueOf("scan/worker_state/REGISTERED")).get(),
                WorkerRecord.class);
        assertEquals("task-1", queriedTasks.get(0).getTaskId());
        assertEquals("worker-1", queriedWorkers.get(0).getWorkerId());
        stateMachine.close();
    }

    private <T> List<T> deserializeList(Message message, Class<T> elementType) throws Exception {
        ObjectInputStream objects = new ObjectInputStream(new ByteArrayInputStream(
                message.getContent().toByteArray()));
        List<?> values = (List<?>) objects.readObject();
        for (Object value : values) {
            elementType.cast(value);
        }
        @SuppressWarnings("unchecked")
        List<T> cast = (List<T>) values;
        return cast;
    }

    private RaftCommand casCommand(TaskRecord record, long expectedVersion) throws Exception {
        return RaftCommand.newBuilder()
                .setType(CommandType.CAS_TASK_RECORD)
                .setTaskRecord(TaskRecordPayload.newBuilder()
                        .setTaskId(record.getTaskId())
                        .setRecord(com.google.protobuf.ByteString.copyFrom(serialize(record)))
                        .setExpectedVersion(expectedVersion)
                        .build())
                .build();
    }

    private byte[] serialize(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream objects = new ObjectOutputStream(bytes);
        objects.writeObject(value);
        objects.flush();
        return bytes.toByteArray();
    }
}
