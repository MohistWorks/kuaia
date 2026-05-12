package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchStateFlusherTest {
    @Test
    void successfulFlushCompletesV2TaskRecordWithCas() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running());
        BatchStateFlusher flusher = new BatchStateFlusher(store);

        flusher.addAck("task-1");
        flusher.flushOnceForTesting();

        assertEquals(TaskState.COMPLETED, store.getTask("task-1").getState());
    }

    @Test
    void failedFlushIsRetriedOnNextFlush() {
        RetryableLegacyStateStore store = new RetryableLegacyStateStore();
        BatchStateFlusher flusher = new BatchStateFlusher(store);

        flusher.addAck("task-1");
        flusher.flushOnceForTesting();
        flusher.flushOnceForTesting();

        assertEquals(2, store.attempts);
        assertEquals("task-1", store.lastTaskId);
        assertEquals(TaskState.COMPLETED, store.lastState);
    }

    @Test
    void missingTaskAckIsDroppedWhenStoreHasNoLegacyState() {
        MissingTaskStateStore store = new MissingTaskStateStore();
        BatchStateFlusher flusher = new BatchStateFlusher(store);

        flusher.addAck("missing-task");
        flusher.flushOnceForTesting();
        flusher.flushOnceForTesting();

        assertEquals(1, store.attempts);
    }

    private static class RetryableLegacyStateStore extends InMemoryStateStore {
        private int attempts;
        private String lastTaskId;
        private TaskState lastState;

        @Override
        public void updateTaskState(String taskId, TaskState state) {
            attempts++;
            if (attempts == 1) {
                throw new RuntimeException("transient store failure");
            }
            lastTaskId = taskId;
            lastState = state;
        }
    }

    private static class MissingTaskStateStore extends InMemoryStateStore {
        private int attempts;

        @Override
        public void updateTaskState(String taskId, TaskState state) {
            attempts++;
            super.updateTaskState(taskId, state);
        }
    }
}
