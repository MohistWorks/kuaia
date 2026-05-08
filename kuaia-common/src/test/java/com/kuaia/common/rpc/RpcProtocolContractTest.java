package com.kuaia.common.rpc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcProtocolContractTest {
    @Test
    void workerMessageCarriesTypedBackpressure() {
        WorkerMessage message = WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setBackpressure(BackpressureSignal.newBuilder()
                        .setLevel(BackpressureLevel.BACKPRESSURE_HIGH)
                        .build())
                .build();

        assertTrue(message.hasBackpressure());
        assertEquals(BackpressureLevel.BACKPRESSURE_HIGH, message.getBackpressure().getLevel());
    }

    @Test
    void workerMessageCarriesTaskAttemptResult() {
        TaskAttemptResult result = TaskAttemptResult.newBuilder()
                .setTaskId("task-1")
                .setAttemptId("attempt-1")
                .setWorkerId("worker-1")
                .setStatus(AttemptStatus.ATTEMPT_SUCCESS)
                .build();

        WorkerMessage message = WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setTaskResult(result)
                .build();

        assertTrue(message.hasTaskResult());
        assertEquals("attempt-1", message.getTaskResult().getAttemptId());
    }

    @Test
    void coordinatorMessageCarriesTaskAssignment() {
        CoordinatorMessage message = CoordinatorMessage.newBuilder()
                .setAssignment(TaskAssignment.newBuilder()
                        .setTaskId("task-1")
                        .setAttemptId("attempt-1")
                        .setStartSeq(42L)
                        .setLeaseUntilMillis(12345L)
                        .build())
                .build();

        assertTrue(message.hasAssignment());
        assertEquals(42L, message.getAssignment().getStartSeq());
    }
}
