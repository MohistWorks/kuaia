package com.kuaia.engine.coordinator.rpc;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import com.kuaia.common.rpc.AttemptStatus;
import com.kuaia.common.rpc.BackpressureLevel;
import com.kuaia.common.rpc.BackpressureSignal;
import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.common.rpc.HeartbeatResponse;
import com.kuaia.common.rpc.RecordAck;
import com.kuaia.common.rpc.TaskAck;
import com.kuaia.common.rpc.TaskAttemptResult;
import com.kuaia.common.rpc.WorkerHeartbeat;
import com.kuaia.common.rpc.WorkerHello;
import com.kuaia.common.rpc.WorkerMessage;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.state.BatchStateFlusher;
import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class CoordinatorServiceImplTest {
    @Test
    void typedAckOnlyMessageDoesNotClearHighBackpressure() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running());
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                new WorkerRegistry(),
                null,
                new TaskAckHandler(store));
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setBackpressure(BackpressureSignal.newBuilder()
                        .setLevel(BackpressureLevel.BACKPRESSURE_HIGH)
                        .build())
                .build());
        assertFalse(service.getStreamManagerForTesting().isAvailable("worker-1"));

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setRecordAck(RecordAck.newBuilder()
                        .setTaskId("task-1")
                        .setAttemptId("attempt-1")
                        .setWorkerId("worker-1")
                        .setSeqId(7L)
                        .setSuccess(true)
                        .build())
                .build());

        assertFalse(service.getStreamManagerForTesting().isAvailable("worker-1"));
        assertEquals(TaskState.RUNNING, store.getTask("task-1").getState());
    }

    @Test
    void typedTaskAttemptResultUpdatesTaskState() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running());
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                new WorkerRegistry(),
                null,
                new TaskAckHandler(store));
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setTaskResult(TaskAttemptResult.newBuilder()
                        .setTaskId("task-1")
                        .setAttemptId("attempt-1")
                        .setWorkerId("worker-1")
                        .setStatus(AttemptStatus.ATTEMPT_SUCCESS)
                        .build())
                .build());

        assertEquals(TaskState.COMPLETED, store.getTask("task-1").getState());
    }

    @Test
    void legacyFailedTaskAckIsNotFlushedAsCompleted() {
        BatchStateFlusher flusher = mock(BatchStateFlusher.class);
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(new WorkerRegistry(), flusher, null);
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setAck(TaskAck.newBuilder()
                        .setTaskId("task-1")
                        .setSuccess(false)
                        .build())
                .build());

        verify(flusher, never()).addAck("task-1");
    }

    @Test
    void streamCompletionUnregistersWorker() {
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(new WorkerRegistry(), null, null);
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setBackpressure(BackpressureSignal.newBuilder()
                        .setLevel(BackpressureLevel.BACKPRESSURE_LOW)
                        .build())
                .build());
        assertTrue(service.getStreamManagerForTesting().isAvailable("worker-1"));

        requestObserver.onCompleted();

        assertFalse(service.getStreamManagerForTesting().isAvailable("worker-1"));
    }

    @Test
    void workerCanReconnectAfterStreamError() {
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(new WorkerRegistry(), null, null);
        StreamObserver<CoordinatorMessage> firstResponseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> firstRequestObserver = service.taskStream(firstResponseObserver);

        firstRequestObserver.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setBackpressure(BackpressureSignal.newBuilder()
                        .setLevel(BackpressureLevel.BACKPRESSURE_HIGH)
                        .build())
                .build());
        assertFalse(service.getStreamManagerForTesting().isAvailable("worker-1"));

        firstRequestObserver.onError(new RuntimeException("stream dropped"));
        assertFalse(service.getStreamManagerForTesting().isAvailable("worker-1"));

        StreamObserver<CoordinatorMessage> secondResponseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> secondRequestObserver = service.taskStream(secondResponseObserver);
        secondRequestObserver.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setBackpressure(BackpressureSignal.newBuilder()
                        .setLevel(BackpressureLevel.BACKPRESSURE_LOW)
                        .build())
                .build());

        assertTrue(service.getStreamManagerForTesting().isAvailable("worker-1"));
    }

    @Test
    void workerHelloReplaysActiveAssignmentsFromRecoveredStateStore() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running()
                .checkpoint("attempt-1", 41L));
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                new WorkerRegistry(),
                null,
                new TaskAckHandler(store),
                store);
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1")
                        .setHost("127.0.0.1")
                        .setPort(9001)
                        .build())
                .build());

        ArgumentCaptor<CoordinatorMessage> messageCaptor = ArgumentCaptor.forClass(CoordinatorMessage.class);
        verify(responseObserver).onNext(messageCaptor.capture());
        CoordinatorMessage message = messageCaptor.getValue();
        assertTrue(message.hasAssignment());
        assertEquals("task-1", message.getAssignment().getTaskId());
        assertEquals("attempt-1", message.getAssignment().getAttemptId());
        assertEquals(42L, message.getAssignment().getStartSeq());
    }

    @Test
    void workerHelloDoesNotReplayRetryingTasksFromRecoveredStateStore() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.saveTask(TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running()
                .retrying("TRANSIENT", "temporary failure"));
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                new WorkerRegistry(),
                null,
                new TaskAckHandler(store),
                store);
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1")
                        .setHost("127.0.0.1")
                        .setPort(9001)
                        .build())
                .build());

        verify(responseObserver, never()).onNext(any(CoordinatorMessage.class));
    }

    @Test
    void workerHelloPersistsOnlineWorkerRecord() {
        InMemoryStateStore store = new InMemoryStateStore();
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                new WorkerRegistry(),
                null,
                null,
                store);
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1")
                        .setHost("127.0.0.1")
                        .setPort(9001)
                        .build())
                .build());

        WorkerRecord worker = store.getWorker("worker-1");
        assertEquals(WorkerRecord.WorkerState.ONLINE, worker.getState());
        assertTrue(worker.isStreamConnected());
        assertEquals(WorkerRecord.BackpressureLevel.LOW, worker.getBackpressureLevel());
        assertEquals("127.0.0.1", worker.getHost());
        assertEquals(9001, worker.getPort());
    }

    @Test
    void backpressurePersistsPausedAndOnlineWorkerStates() {
        InMemoryStateStore store = new InMemoryStateStore();
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                new WorkerRegistry(),
                null,
                null,
                store);
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1")
                        .setHost("127.0.0.1")
                        .setPort(9001)
                        .build())
                .build());
        requestObserver.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setBackpressure(BackpressureSignal.newBuilder()
                        .setLevel(BackpressureLevel.BACKPRESSURE_HIGH)
                        .build())
                .build());

        WorkerRecord paused = store.getWorker("worker-1");
        assertEquals(WorkerRecord.WorkerState.PAUSED, paused.getState());
        assertTrue(paused.isStreamConnected());
        assertEquals(WorkerRecord.BackpressureLevel.HIGH, paused.getBackpressureLevel());

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setBackpressure(BackpressureSignal.newBuilder()
                        .setLevel(BackpressureLevel.BACKPRESSURE_LOW)
                        .build())
                .build());

        WorkerRecord online = store.getWorker("worker-1");
        assertEquals(WorkerRecord.WorkerState.ONLINE, online.getState());
        assertTrue(online.isStreamConnected());
        assertEquals(WorkerRecord.BackpressureLevel.LOW, online.getBackpressureLevel());
    }

    @Test
    void workerHelloClearsStaleStreamBackpressure() {
        InMemoryStateStore store = new InMemoryStateStore();
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                new WorkerRegistry(),
                null,
                null,
                store);
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1")
                        .setHost("127.0.0.1")
                        .setPort(9001)
                        .build())
                .build());
        requestObserver.onNext(WorkerMessage.newBuilder()
                .setWorkerId("worker-1")
                .setBackpressure(BackpressureSignal.newBuilder()
                        .setLevel(BackpressureLevel.BACKPRESSURE_HIGH)
                        .build())
                .build());
        assertFalse(service.getStreamManagerForTesting().isAvailable("worker-1"));

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1")
                        .setHost("127.0.0.1")
                        .setPort(9001)
                        .build())
                .build());

        assertTrue(service.getStreamManagerForTesting().isAvailable("worker-1"));
        WorkerRecord worker = store.getWorker("worker-1");
        assertEquals(WorkerRecord.WorkerState.ONLINE, worker.getState());
        assertTrue(worker.isStreamConnected());
        assertEquals(WorkerRecord.BackpressureLevel.LOW, worker.getBackpressureLevel());
    }

    @Test
    void streamCompletionPersistsOfflineWorkerRecord() {
        InMemoryStateStore store = new InMemoryStateStore();
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                new WorkerRegistry(),
                null,
                null,
                store);
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1")
                        .setHost("127.0.0.1")
                        .setPort(9001)
                        .build())
                .build());
        requestObserver.onCompleted();

        WorkerRecord worker = store.getWorker("worker-1");
        assertEquals(WorkerRecord.WorkerState.OFFLINE, worker.getState());
        assertFalse(worker.isStreamConnected());
    }

    @Test
    void emptyMessageDoesNotClearStreamWorkerIdentityBeforeCompletion() {
        InMemoryStateStore store = new InMemoryStateStore();
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                new WorkerRegistry(),
                null,
                null,
                store);
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1")
                        .setHost("127.0.0.1")
                        .setPort(9001)
                        .build())
                .build());
        requestObserver.onNext(WorkerMessage.newBuilder().build());
        requestObserver.onCompleted();

        assertFalse(service.getStreamManagerForTesting().isAvailable("worker-1"));
        WorkerRecord worker = store.getWorker("worker-1");
        assertEquals(WorkerRecord.WorkerState.OFFLINE, worker.getState());
        assertFalse(worker.isStreamConnected());
    }

    @Test
    void heartbeatPersistsWorkerLoadAndHeartbeatTime() {
        InMemoryStateStore store = new InMemoryStateStore();
        CoordinatorServiceImpl service = new CoordinatorServiceImpl(
                new WorkerRegistry(),
                null,
                null,
                store);
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);
        StreamObserver<WorkerMessage> requestObserver = service.taskStream(responseObserver);

        requestObserver.onNext(WorkerMessage.newBuilder()
                .setHello(WorkerHello.newBuilder()
                        .setWorkerId("worker-1")
                        .setHost("127.0.0.1")
                        .setPort(9001)
                        .build())
                .build());
        long previousHeartbeat = store.getWorker("worker-1").getLastHeartbeatMillis();
        StreamObserver<HeartbeatResponse> heartbeatObserver = mock(StreamObserver.class);

        service.heartbeat(WorkerHeartbeat.newBuilder()
                        .setId("worker-1")
                        .setCpuLoad(0.2)
                        .setMemLoad(0.6)
                        .build(),
                heartbeatObserver);

        WorkerRecord worker = store.getWorker("worker-1");
        assertEquals(0.4, worker.getLoadScore(), 0.0001);
        assertTrue(worker.getLastHeartbeatMillis() >= previousHeartbeat);
        assertEquals(WorkerRecord.WorkerState.ONLINE, worker.getState());
        assertTrue(worker.isStreamConnected());
    }
}
