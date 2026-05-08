package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

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
}
