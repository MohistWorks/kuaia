package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.rpc.AttemptStatus;
import com.kuaia.common.rpc.CheckpointAck;
import com.kuaia.common.rpc.RecordAck;
import com.kuaia.common.rpc.TaskAttemptResult;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskAckHandlerTest {
    @Test
    void staleAttemptResultDoesNotCompleteTask() {
        InMemoryStateStore store = new InMemoryStateStore();
        TaskRecord current = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-2", "attempt-2", System.currentTimeMillis() + 20_000L)
                .running();
        store.saveTask(current);
        TaskAckHandler handler = new TaskAckHandler(store);

        boolean accepted = handler.handleTaskAttemptResult(TaskAttemptResult.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("attempt-1")
                .setWorkerId("worker-1")
                .setStatus(AttemptStatus.ATTEMPT_SUCCESS)
                .build());

        assertFalse(accepted);
        assertEquals(TaskState.RUNNING, store.getTask("task-1").getState());
    }

    @Test
    void currentAttemptSuccessCompletesTask() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", System.currentTimeMillis() + 10_000L)
                .running());
        TaskAckHandler handler = new TaskAckHandler(store);

        boolean accepted = handler.handleTaskAttemptResult(TaskAttemptResult.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("attempt-1")
                .setWorkerId("worker-1")
                .setStatus(AttemptStatus.ATTEMPT_SUCCESS)
                .build());

        assertTrue(accepted);
        assertEquals(TaskState.COMPLETED, store.getTask("task-1").getState());
    }

    @Test
    void expiredAttemptResultDoesNotCompleteTask() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", System.currentTimeMillis() - 1_000L)
                .running());
        TaskAckHandler handler = new TaskAckHandler(store);

        boolean accepted = handler.handleTaskAttemptResult(TaskAttemptResult.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("attempt-1")
                .setWorkerId("worker-1")
                .setStatus(AttemptStatus.ATTEMPT_SUCCESS)
                .build());

        assertFalse(accepted);
        assertEquals(TaskState.RUNNING, store.getTask("task-1").getState());
    }

    @Test
    void duplicateTaskAttemptResultDoesNotMutateCompletedTask() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", System.currentTimeMillis() + 10_000L)
                .running());
        TaskAckHandler handler = new TaskAckHandler(store);
        TaskAttemptResult result = TaskAttemptResult.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("attempt-1")
                .setWorkerId("worker-1")
                .setStatus(AttemptStatus.ATTEMPT_SUCCESS)
                .build();

        assertTrue(handler.handleTaskAttemptResult(result));
        long completedVersion = store.getTask("task-1").getVersion();

        assertFalse(handler.handleTaskAttemptResult(result));
        assertEquals(TaskState.COMPLETED, store.getTask("task-1").getState());
        assertEquals(completedVersion, store.getTask("task-1").getVersion());
    }

    @Test
    void currentAttemptFailureFailsTask() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", System.currentTimeMillis() + 10_000L)
                .running());
        TaskAckHandler handler = new TaskAckHandler(store);

        boolean accepted = handler.handleTaskAttemptResult(TaskAttemptResult.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("attempt-1")
                .setWorkerId("worker-1")
                .setStatus(AttemptStatus.ATTEMPT_FAILED)
                .setErrorCode("WORKER_ERROR")
                .setErrorMessage("boom")
                .build());

        TaskRecord record = store.getTask("task-1");
        assertTrue(accepted);
        assertEquals(TaskState.FAILED, record.getState());
        assertEquals("WORKER_ERROR", record.getLastErrorCode());
        assertEquals("boom", record.getLastErrorMessage());
    }

    @Test
    void currentAttemptCancellationCancelsTask() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", System.currentTimeMillis() + 10_000L)
                .running());
        TaskAckHandler handler = new TaskAckHandler(store);

        boolean accepted = handler.handleTaskAttemptResult(TaskAttemptResult.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("attempt-1")
                .setWorkerId("worker-1")
                .setStatus(AttemptStatus.ATTEMPT_CANCELLED)
                .setErrorMessage("interrupted")
                .build());

        TaskRecord record = store.getTask("task-1");
        assertTrue(accepted);
        assertEquals(TaskState.CANCELLED, record.getState());
        assertEquals("interrupted", record.getLastErrorMessage());
    }

    @Test
    void checkpointAckOnlyAdvancesForCurrentAttempt() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", System.currentTimeMillis() + 10_000L)
                .running());
        TaskAckHandler handler = new TaskAckHandler(store);

        assertTrue(handler.handleCheckpointAck(CheckpointAck.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("attempt-1")
                .setWorkerId("worker-1")
                .setProcessedSeq(25L)
                .build()));
        assertEquals(25L, store.getTask("task-1").getLastCheckpointSeq());

        assertFalse(handler.handleCheckpointAck(CheckpointAck.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("attempt-old")
                .setWorkerId("worker-1")
                .setProcessedSeq(99L)
                .build()));
        assertEquals(25L, store.getTask("task-1").getLastCheckpointSeq());
    }

    @Test
    void recordAckDoesNotCompleteTask() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", System.currentTimeMillis() + 10_000L)
                .running());
        TaskAckHandler handler = new TaskAckHandler(store);

        assertTrue(handler.handleRecordAck(RecordAck.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("attempt-1")
                .setWorkerId("worker-1")
                .setSeqId(7L)
                .setSuccess(true)
                .build()));
        assertEquals(TaskState.RUNNING, store.getTask("task-1").getState());
    }
}
