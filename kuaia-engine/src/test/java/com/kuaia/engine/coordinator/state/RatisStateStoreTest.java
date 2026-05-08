package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.raft.CommandType;
import com.kuaia.common.raft.RaftCommand;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
