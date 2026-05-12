package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import com.kuaia.common.raft.CommandType;
import com.kuaia.common.raft.RaftCommand;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentCaptor.forClass;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RatisStateStoreTest {
    @Test
    void updateTaskStateRejectsFailedRaftWriteReply() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        RaftClientReply failedReply = mock(RaftClientReply.class);
        when(client.io()).thenReturn(io);
        when(failedReply.isSuccess()).thenReturn(false);
        when(io.send(any(Message.class))).thenReturn(failedReply);

        RatisStateStore store = new RatisStateStore(client);

        assertThrows(RuntimeException.class, () -> store.updateTaskState("task-1", TaskState.CREATED));
    }

    @Test
    void saveTaskRecordSendsFullTaskRecordCommand() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        RaftClientReply success = mock(RaftClientReply.class);
        when(client.io()).thenReturn(io);
        when(success.isSuccess()).thenReturn(true);
        when(io.send(any(Message.class))).thenReturn(success);
        ArgumentCaptor<Message> messageCaptor = forClass(Message.class);

        TaskRecord record = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();
        new RatisStateStore(client).saveTask(record);

        verify(io).send(messageCaptor.capture());
        RaftCommand command = RaftCommand.parseFrom(messageCaptor.getValue().getContent().toByteArray());
        assertEquals(CommandType.SAVE_TASK_RECORD, command.getType());
        assertEquals("task-1", command.getTaskRecord().getTaskId());
    }

    @Test
    void compareAndSetTaskSendsCasTaskRecordCommand() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        RaftClientReply success = mock(RaftClientReply.class);
        when(client.io()).thenReturn(io);
        when(success.isSuccess()).thenReturn(true);
        when(io.send(any(Message.class))).thenReturn(success);
        ArgumentCaptor<Message> messageCaptor = forClass(Message.class);

        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();
        TaskRecord completed = running.complete("attempt-1");

        assertTrue(new RatisStateStore(client).compareAndSetTask(running, completed));

        verify(io).send(messageCaptor.capture());
        RaftCommand command = RaftCommand.parseFrom(messageCaptor.getValue().getContent().toByteArray());
        assertEquals(CommandType.CAS_TASK_RECORD, command.getType());
        assertEquals(running.getVersion(), command.getTaskRecord().getExpectedVersion());
    }

    @Test
    void compareAndSetTaskReturnsFalseForRejectedCasReply() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        RaftClientReply rejected = successMessageReply(RocksDBStateMachine.CAS_REJECTED);
        when(client.io()).thenReturn(io);
        when(io.send(any(Message.class))).thenReturn(rejected);

        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();

        boolean updated = new RatisStateStore(client)
                .compareAndSetTask(running, running.complete("attempt-1"));

        assertFalse(updated);
    }

    @Test
    void saveWorkerSendsWorkerRecordCommand() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        RaftClientReply success = mock(RaftClientReply.class);
        when(client.io()).thenReturn(io);
        when(success.isSuccess()).thenReturn(true);
        when(io.send(any(Message.class))).thenReturn(success);
        ArgumentCaptor<Message> messageCaptor = forClass(Message.class);

        WorkerRecord worker = WorkerRecord.registered("worker-1", "127.0.0.1", 9001);
        new RatisStateStore(client).saveWorker(worker);

        verify(io).send(messageCaptor.capture());
        RaftCommand command = RaftCommand.parseFrom(messageCaptor.getValue().getContent().toByteArray());
        assertEquals(CommandType.SAVE_WORKER_RECORD, command.getType());
        assertEquals("worker-1", command.getWorkerRecord().getWorkerId());
    }

    @Test
    void getWorkerReadsWorkerRecordByKey() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        WorkerRecord expected = WorkerRecord.registered("worker-1", "127.0.0.1", 9001);
        RaftClientReply success = successReply(expected);
        when(client.io()).thenReturn(io);
        when(io.sendReadOnly(any(Message.class))).thenReturn(success);
        ArgumentCaptor<Message> messageCaptor = forClass(Message.class);

        WorkerRecord worker = new RatisStateStore(client).getWorker("worker-1");

        assertEquals("worker-1", worker.getWorkerId());
        verify(io).sendReadOnly(messageCaptor.capture());
        assertEquals("worker/worker-1", messageCaptor.getValue().getContent().toStringUtf8());
    }

    @Test
    void legacyGetTaskStateReadsTaskRecordByKey() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();
        RaftClientReply success = successReply(running);
        when(client.io()).thenReturn(io);
        when(io.sendReadOnly(any(Message.class))).thenReturn(success);
        ArgumentCaptor<Message> messageCaptor = forClass(Message.class);

        TaskState state = new RatisStateStore(client).getTaskState("task-1");

        assertEquals(TaskState.RUNNING, state);
        verify(io).sendReadOnly(messageCaptor.capture());
        assertEquals("task/task-1", messageCaptor.getValue().getContent().toStringUtf8());
    }

    @Test
    void scanTasksByStateReadsRaftScanQuery() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        RaftClientReply success = successReply(Arrays.asList(
                TaskRecord.created("job-1", "task-1"),
                TaskRecord.created("job-1", "task-2")));
        when(client.io()).thenReturn(io);
        when(io.sendReadOnly(any(Message.class))).thenReturn(success);
        ArgumentCaptor<Message> messageCaptor = forClass(Message.class);

        List<TaskRecord> records = new RatisStateStore(client).scanTasksByState(TaskState.CREATED);

        assertEquals(2, records.size());
        verify(io).sendReadOnly(messageCaptor.capture());
        assertEquals("scan/task_state/CREATED", messageCaptor.getValue().getContent().toStringUtf8());
    }

    @Test
    void legacyGetTasksByStateUsesTaskScanQuery() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        RaftClientReply success = successReply(Arrays.asList(TaskRecord.created("job-1", "task-1")));
        when(client.io()).thenReturn(io);
        when(io.sendReadOnly(any(Message.class))).thenReturn(success);
        ArgumentCaptor<Message> messageCaptor = forClass(Message.class);

        List<TaskDefinition> definitions = new RatisStateStore(client).getTasksByState(TaskState.CREATED);

        assertTrue(definitions.isEmpty());
        verify(io).sendReadOnly(messageCaptor.capture());
        assertEquals("scan/task_state/CREATED", messageCaptor.getValue().getContent().toStringUtf8());
    }

    @Test
    void scanActiveTasksByWorkerReadsRaftScanQuery() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();
        RaftClientReply success = successReply(Arrays.asList(running));
        when(client.io()).thenReturn(io);
        when(io.sendReadOnly(any(Message.class))).thenReturn(success);
        ArgumentCaptor<Message> messageCaptor = forClass(Message.class);

        List<TaskRecord> records = new RatisStateStore(client).scanActiveTasksByWorker("worker-1");

        assertEquals(1, records.size());
        verify(io).sendReadOnly(messageCaptor.capture());
        assertEquals("scan/task_worker/worker-1", messageCaptor.getValue().getContent().toStringUtf8());
    }

    @Test
    void scanWorkersByStateReadsRaftScanQuery() throws Exception {
        RaftClient client = mock(RaftClient.class);
        BlockingApi io = mock(BlockingApi.class);
        RaftClientReply success = successReply(Arrays.asList(
                WorkerRecord.registered("worker-1", "127.0.0.1", 9001)));
        when(client.io()).thenReturn(io);
        when(io.sendReadOnly(any(Message.class))).thenReturn(success);
        ArgumentCaptor<Message> messageCaptor = forClass(Message.class);

        List<WorkerRecord> workers = new RatisStateStore(client)
                .scanWorkersByState(WorkerRecord.WorkerState.REGISTERED);

        assertEquals(1, workers.size());
        verify(io).sendReadOnly(messageCaptor.capture());
        assertEquals("scan/worker_state/REGISTERED", messageCaptor.getValue().getContent().toStringUtf8());
    }

    private RaftClientReply successReply(Object value) throws Exception {
        RaftClientReply reply = mock(RaftClientReply.class);
        when(reply.isSuccess()).thenReturn(true);
        when(reply.getMessage()).thenReturn(Message.valueOf(
                org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(serialize(value))));
        return reply;
    }

    private RaftClientReply successMessageReply(String value) {
        RaftClientReply reply = mock(RaftClientReply.class);
        when(reply.isSuccess()).thenReturn(true);
        when(reply.getMessage()).thenReturn(Message.valueOf(value));
        return reply;
    }

    private byte[] serialize(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream objects = new ObjectOutputStream(bytes);
        objects.writeObject(value);
        objects.flush();
        return bytes.toByteArray();
    }
}
