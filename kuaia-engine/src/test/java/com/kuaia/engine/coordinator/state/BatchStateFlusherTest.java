package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchStateFlusherTest {
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
}
