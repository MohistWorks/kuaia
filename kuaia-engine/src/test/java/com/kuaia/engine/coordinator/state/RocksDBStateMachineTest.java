package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import com.kuaia.common.raft.CommandType;
import com.kuaia.common.raft.RaftCommand;
import com.kuaia.common.raft.SubmitJobPayload;
import com.kuaia.common.raft.TaskRecordPayload;
import org.apache.ratis.protocol.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

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

        assertTrue(stateMachine.applyTaskRecord(running, false, -1L));
        TaskRecord checkpointed = running.checkpoint("attempt-1", 10L);
        assertTrue(stateMachine.applyTaskRecord(checkpointed, true, running.getVersion()));
        assertFalse(stateMachine.applyTaskRecord(running.complete("attempt-1"), true, running.getVersion()));

        TaskRecord stored = stateMachine.getTaskRecord("task-1");
        assertEquals(TaskState.RUNNING, stored.getState());
        assertEquals(10L, stored.getLastCheckpointSeq());
        stateMachine.close();
    }

    @Test
    void staleCasCommandReturnsRejectedMessageWithoutOverwritingRecord() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());
        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();
        TaskRecord checkpointed = running.checkpoint("attempt-1", 10L);

        assertTrue(stateMachine.applyTaskRecord(running, false, -1L));
        assertTrue(stateMachine.applyTaskRecord(checkpointed, true, running.getVersion()));

        Message result = stateMachine
                .applyTaskRecordCommand(casCommand(running.complete("attempt-1"), running.getVersion()))
                .get();

        assertEquals(RocksDBStateMachine.CAS_REJECTED, result.getContent().toStringUtf8());
        TaskRecord stored = stateMachine.getTaskRecord("task-1");
        assertEquals(TaskState.RUNNING, stored.getState());
        assertEquals(10L, stored.getLastCheckpointSeq());
        stateMachine.close();
    }

    @Test
    void scansTaskAndWorkerRecordsForStateStoreContract() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());
        TaskRecord running = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();
        TaskRecord completed = TaskRecord.created("job-1", "task-2")
                .dispatching("worker-1", "attempt-2", 10_000L)
                .running()
                .complete("attempt-2");

        assertTrue(stateMachine.applyTaskRecord(running, false, -1L));
        assertTrue(stateMachine.applyTaskRecord(completed, false, -1L));
        stateMachine.applyWorkerRecord(WorkerRecord.registered("worker-1", "127.0.0.1", 9001));
        stateMachine.applyWorkerRecord(WorkerRecord.registered("worker-2", "127.0.0.1", 9002)
                .withState(WorkerRecord.WorkerState.OFFLINE));

        assertEquals(1, stateMachine.scanTaskRecordsByState(TaskState.RUNNING).size());
        assertEquals(1, stateMachine.scanActiveTaskRecordsByWorker("worker-1").size());
        assertEquals("worker-1", stateMachine.getWorkerRecord("worker-1").getWorkerId());
        assertEquals(1, stateMachine.scanWorkerRecordsByState(WorkerRecord.WorkerState.REGISTERED).size());

        List<TaskRecord> queriedTasks = deserializeList(
                stateMachine.query(Message.valueOf("scan/task_state/RUNNING")).get(),
                TaskRecord.class);
        List<WorkerRecord> queriedWorkers = deserializeList(
                stateMachine.query(Message.valueOf("scan/worker_state/REGISTERED")).get(),
                WorkerRecord.class);
        assertEquals("task-1", queriedTasks.get(0).getTaskId());
        assertEquals("worker-1", queriedWorkers.get(0).getWorkerId());
        stateMachine.close();
    }

    @Test
    void retryingTasksAreNotReturnedAsActiveWorkerAssignments() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());
        TaskRecord retrying = TaskRecord.created("job-1", "task-1")
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running()
                .retrying("TRANSIENT", "temporary failure");

        assertTrue(stateMachine.applyTaskRecord(retrying, false, -1L));

        assertEquals(0, stateMachine.scanActiveTaskRecordsByWorker("worker-1").size());
        assertEquals(1, stateMachine.scanTaskRecordsByState(TaskState.RETRYING).size());
        stateMachine.close();
    }

    @Test
    void cascadeJobStateWhenAllTasksCompleted() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());

        String jobId = "job-1";
        String task1 = "task-1";
        String task2 = "task-2";

        JobInstance job = new JobInstance();
        job.setJobId(jobId);
        job.setTaskIds(Arrays.asList(task1, task2));

        stateMachine.applySubmitJob(SubmitJobPayload.newBuilder()
                .setJobId(jobId)
                .setDefinition(com.google.protobuf.ByteString.copyFrom(serialize(job)))
                .build());

        TaskRecord running1 = TaskRecord.created(jobId, task1)
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();
        TaskRecord running2 = TaskRecord.created(jobId, task2)
                .dispatching("worker-1", "attempt-2", 10_000L)
                .running();

        stateMachine.applyTaskRecord(running1, false, -1L);
        stateMachine.applyTaskRecord(running2, false, -1L);

        // Sync task-1 to COMPLETED — cascade should not fire yet (task-2 still RUNNING)
        stateMachine.syncTaskRecordState(task1, TaskState.COMPLETED.ordinal());
        stateMachine.evaluateJobState(task1);

        assertEquals(TaskState.COMPLETED, stateMachine.getTaskRecord(task1).getState());
        JobInstance midJob = stateMachine.getJobInstance(jobId);
        assertEquals(TaskState.CREATED, midJob.getState());

        // Sync task-2 to COMPLETED — cascade should fire (all tasks done)
        stateMachine.syncTaskRecordState(task2, TaskState.COMPLETED.ordinal());
        stateMachine.evaluateJobState(task2);

        assertEquals(TaskState.COMPLETED, stateMachine.getTaskRecord(task2).getState());
        JobInstance finalJob = stateMachine.getJobInstance(jobId);
        assertEquals(TaskState.COMPLETED, finalJob.getState());

        stateMachine.close();
    }

    @Test
    void cascadeJobStateToFinishedWithErrorsWhenSomeTasksFail() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());

        String jobId = "job-2";
        String task1 = "task-1";
        String task2 = "task-2";

        JobInstance job = new JobInstance();
        job.setJobId(jobId);
        job.setTaskIds(Arrays.asList(task1, task2));

        stateMachine.applySubmitJob(SubmitJobPayload.newBuilder()
                .setJobId(jobId)
                .setDefinition(com.google.protobuf.ByteString.copyFrom(serialize(job)))
                .build());

        TaskRecord running1 = TaskRecord.created(jobId, task1)
                .dispatching("worker-1", "attempt-1", 10_000L)
                .running();
        TaskRecord running2 = TaskRecord.created(jobId, task2)
                .dispatching("worker-1", "attempt-2", 10_000L)
                .running();

        stateMachine.applyTaskRecord(running1, false, -1L);
        stateMachine.applyTaskRecord(running2, false, -1L);

        stateMachine.syncTaskRecordState(task1, TaskState.COMPLETED.ordinal());
        stateMachine.evaluateJobState(task1);

        stateMachine.syncTaskRecordState(task2, TaskState.FAILED.ordinal());
        stateMachine.evaluateJobState(task2);

        JobInstance finalJob = stateMachine.getJobInstance(jobId);
        assertEquals(TaskState.FINISHED_WITH_ERRORS, finalJob.getState());

        stateMachine.close();
    }

    @Test
    void updateStateSkipsCascadeWhenTaskRecordNotFound() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());

        String jobId = "job-1";
        JobInstance job = new JobInstance();
        job.setJobId(jobId);
        job.setTaskIds(List.of("task-1"));

        stateMachine.applySubmitJob(SubmitJobPayload.newBuilder()
                .setJobId(jobId)
                .setDefinition(com.google.protobuf.ByteString.copyFrom(serialize(job)))
                .build());

        // syncTaskRecordState returns silently when TaskRecord doesn't exist
        stateMachine.syncTaskRecordState("task-1", TaskState.COMPLETED.ordinal());
        // evaluateJobState returns silently when TaskRecord not found
        stateMachine.evaluateJobState("task-1");

        JobInstance midJob = stateMachine.getJobInstance(jobId);
        assertEquals(TaskState.CREATED, midJob.getState());

        stateMachine.close();
    }

    @Test
    void jobCascadesToCompletedViaTaskRecordPath() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());

        String jobId = "job-real-1";
        String task1 = "task-1";
        String task2 = "task-2";

        JobInstance job = new JobInstance();
        job.setJobId(jobId);
        job.setTaskIds(Arrays.asList(task1, task2));
        stateMachine.applySubmitJob(SubmitJobPayload.newBuilder()
                .setJobId(jobId)
                .setDefinition(com.google.protobuf.ByteString.copyFrom(serialize(job)))
                .build());

        // Production path: persist terminal TaskRecords directly; the cascade must fire on its own,
        // without any manual evaluateJobState call.
        TaskRecord done1 = TaskRecord.created(jobId, task1)
                .dispatching("worker-1", "a1", 10_000L).running().complete("a1");
        TaskRecord done2 = TaskRecord.created(jobId, task2)
                .dispatching("worker-1", "a2", 10_000L).running().complete("a2");

        stateMachine.applyTaskRecord(done1, false, -1L);
        assertEquals(TaskState.CREATED, stateMachine.getJobInstance(jobId).getState());
        assertEquals(1, stateMachine.getJobInstance(jobId).getCompletedTasks());

        stateMachine.applyTaskRecord(done2, false, -1L);
        assertEquals(TaskState.COMPLETED, stateMachine.getJobInstance(jobId).getState());
        assertEquals(2, stateMachine.getJobInstance(jobId).getCompletedTasks());

        stateMachine.close();
    }

    @Test
    void replayingSameTerminalRecordDoesNotDoubleCount() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());

        String jobId = "job-replay";
        JobInstance job = new JobInstance();
        job.setJobId(jobId);
        job.setTaskIds(Arrays.asList("task-1", "task-2"));
        stateMachine.applySubmitJob(SubmitJobPayload.newBuilder()
                .setJobId(jobId)
                .setDefinition(com.google.protobuf.ByteString.copyFrom(serialize(job)))
                .build());

        TaskRecord done1 = TaskRecord.created(jobId, "task-1")
                .dispatching("w", "a1", 10_000L).running().complete("a1");

        // Apply the same terminal record three times (simulating Raft log replay). The counter must
        // stay at 1 and the job must not be wrongly finalized.
        stateMachine.applyTaskRecord(done1, false, -1L);
        stateMachine.applyTaskRecord(done1, false, -1L);
        stateMachine.applyTaskRecord(done1, false, -1L);
        assertEquals(1, stateMachine.getJobInstance(jobId).getCompletedTasks());
        assertEquals(TaskState.CREATED, stateMachine.getJobInstance(jobId).getState());

        TaskRecord done2 = TaskRecord.created(jobId, "task-2")
                .dispatching("w", "a2", 10_000L).running().complete("a2");
        stateMachine.applyTaskRecord(done2, false, -1L);
        assertEquals(2, stateMachine.getJobInstance(jobId).getCompletedTasks());
        assertEquals(TaskState.COMPLETED, stateMachine.getJobInstance(jobId).getState());

        // A late replay of an already-terminal task is a no-op (state unchanged).
        stateMachine.applyTaskRecord(done1, false, -1L);
        assertEquals(2, stateMachine.getJobInstance(jobId).getCompletedTasks());
        assertEquals(TaskState.COMPLETED, stateMachine.getJobInstance(jobId).getState());

        stateMachine.close();
    }

    @Test
    void rerunningAFailedTaskRecountsJobFromErrorsToCompleted() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());

        String jobId = "job-recount";
        JobInstance job = new JobInstance();
        job.setJobId(jobId);
        job.setTaskIds(Arrays.asList("task-1", "task-2"));
        stateMachine.applySubmitJob(SubmitJobPayload.newBuilder()
                .setJobId(jobId)
                .setDefinition(com.google.protobuf.ByteString.copyFrom(serialize(job)))
                .build());

        TaskRecord done1 = TaskRecord.created(jobId, "task-1")
                .dispatching("w", "a1", 10_000L).running().complete("a1");
        TaskRecord failed2 = TaskRecord.created(jobId, "task-2")
                .dispatching("w", "a2", 10_000L).running().fail("a2", "ERR", "boom");
        stateMachine.applyTaskRecord(done1, false, -1L);
        stateMachine.applyTaskRecord(failed2, false, -1L);
        assertEquals(TaskState.FINISHED_WITH_ERRORS, stateMachine.getJobInstance(jobId).getState());
        assertEquals(1, stateMachine.getJobInstance(jobId).getFailedTasks());

        // Operator re-runs the failed task and it now succeeds; the failed bucket is decremented and
        // the completed bucket incremented, recounting the job to COMPLETED.
        TaskRecord recovered2 = TaskRecord.created(jobId, "task-2")
                .dispatching("w", "a3", 10_000L).running().complete("a3");
        stateMachine.applyTaskRecord(recovered2, false, -1L);
        assertEquals(0, stateMachine.getJobInstance(jobId).getFailedTasks());
        assertEquals(2, stateMachine.getJobInstance(jobId).getCompletedTasks());
        assertEquals(TaskState.COMPLETED, stateMachine.getJobInstance(jobId).getState());

        stateMachine.close();
    }

    @Test
    void jobCascadesToFinishedWithErrorsViaCasCommandPath() throws Exception {
        RocksDBStateMachine stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(tempDir.toString());

        String jobId = "job-real-2";
        String task1 = "task-1";
        String task2 = "task-2";

        JobInstance job = new JobInstance();
        job.setJobId(jobId);
        job.setTaskIds(Arrays.asList(task1, task2));
        stateMachine.applySubmitJob(SubmitJobPayload.newBuilder()
                .setJobId(jobId)
                .setDefinition(com.google.protobuf.ByteString.copyFrom(serialize(job)))
                .build());

        TaskRecord done1 = TaskRecord.created(jobId, task1)
                .dispatching("worker-1", "a1", 10_000L).running().complete("a1");
        stateMachine.applyTaskRecord(done1, false, -1L);
        // One task still running -> no premature finalize.
        assertEquals(TaskState.CREATED, stateMachine.getJobInstance(jobId).getState());

        // Drive task-2 to FAILED through the production CAS command path (save running, then CAS to failed).
        TaskRecord running2 = TaskRecord.created(jobId, task2)
                .dispatching("worker-1", "a2", 10_000L).running();
        stateMachine.applyTaskRecord(running2, false, -1L);
        TaskRecord failed2 = running2.fail("a2", "ERR", "boom");
        String reply = stateMachine.applyTaskRecordCommand(casCommand(failed2, running2.getVersion()))
                .get().getContent().toStringUtf8();
        assertEquals("OK", reply);

        assertEquals(TaskState.FINISHED_WITH_ERRORS, stateMachine.getJobInstance(jobId).getState());

        stateMachine.close();
    }

    private <T> List<T> deserializeList(Message message, Class<T> elementType) throws Exception {
        ObjectInputStream objects = new ObjectInputStream(new ByteArrayInputStream(
                message.getContent().toByteArray()));
        List<?> values = (List<?>) objects.readObject();
        for (Object value : values) {
            elementType.cast(value);
        }
        @SuppressWarnings("unchecked")
        List<T> cast = (List<T>) values;
        return cast;
    }

    private RaftCommand casCommand(TaskRecord record, long expectedVersion) throws Exception {
        return RaftCommand.newBuilder()
                .setType(CommandType.CAS_TASK_RECORD)
                .setTaskRecord(TaskRecordPayload.newBuilder()
                        .setTaskId(record.getTaskId())
                        .setRecord(com.google.protobuf.ByteString.copyFrom(serialize(record)))
                        .setExpectedVersion(expectedVersion)
                        .build())
                .build();
    }

    private byte[] serialize(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream objects = new ObjectOutputStream(bytes);
        objects.writeObject(value);
        objects.flush();
        return bytes.toByteArray();
    }
}
