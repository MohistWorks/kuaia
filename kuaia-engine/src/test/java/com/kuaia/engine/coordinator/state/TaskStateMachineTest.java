package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskStateMachineTest {
    @Test
    void taskCanMoveFromCreatedToRunningThroughDispatching() {
        TaskRecord record = TaskRecord.created("job-1", "task-1");

        TaskRecord dispatching = record.dispatching("worker-1", "attempt-1", 10_000L);
        TaskRecord running = dispatching.running();

        assertEquals(TaskState.RUNNING, running.getState());
        assertEquals("worker-1", running.getAssignedWorkerId());
        assertEquals("attempt-1", running.getAttemptId());
        assertEquals(2, running.getVersion());
    }

    @Test
    void staleAttemptCannotCompleteTask() {
        TaskRecord record = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running()
                .retrying("transient", "HTTP 503")
                .dispatching("worker-2", "attempt-2", 20_000L)
                .running();

        assertThrows(IllegalArgumentException.class, () -> record.complete("attempt-1"));
        assertEquals(TaskState.COMPLETED, record.complete("attempt-2").getState());
    }
}
