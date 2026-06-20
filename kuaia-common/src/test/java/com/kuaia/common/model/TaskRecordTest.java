package com.kuaia.common.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskRecordTest {
    @Test
    void failExhaustedFromRunningProducesFailed() {
        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "a1", 10_000L)
                .running();
        TaskRecord failed = running.failExhausted("RETRY_EXHAUSTED", "attempts=4");
        assertEquals(TaskState.FAILED, failed.getState());
        assertEquals("RETRY_EXHAUSTED", failed.getLastErrorCode());
        assertEquals(running.getAttemptNo(), failed.getAttemptNo());
    }

    @Test
    void failExhaustedFromDispatchingProducesFailed() {
        TaskRecord dispatching = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "a1", 10_000L);
        TaskRecord failed = dispatching.failExhausted("RETRY_EXHAUSTED", "lease expired; attempts=4");
        assertEquals(TaskState.FAILED, failed.getState());
    }

    @Test
    void failExhaustedRejectsTerminalState() {
        TaskRecord completed = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "a1", 10_000L)
                .running()
                .complete("a1");
        assertThrows(IllegalStateException.class,
                () -> completed.failExhausted("RETRY_EXHAUSTED", "x"));
    }
}
