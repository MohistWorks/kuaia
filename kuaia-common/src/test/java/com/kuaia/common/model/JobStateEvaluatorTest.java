package com.kuaia.common.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobStateEvaluatorTest {

    @Test
    void emptyWhenAnyTaskStillActive() {
        // Design §2.1: decide only once all tasks have left active states.
        assertTrue(JobStateEvaluator.evaluate(List.of(TaskState.COMPLETED, TaskState.RUNNING)).isEmpty());
        assertTrue(JobStateEvaluator.evaluate(List.of(TaskState.CREATED)).isEmpty());
        assertTrue(JobStateEvaluator.evaluate(List.of(TaskState.DISPATCHING, TaskState.COMPLETED)).isEmpty());
        // A failed task does NOT prematurely finalize while a sibling is still running.
        assertTrue(JobStateEvaluator.evaluate(List.of(TaskState.FAILED, TaskState.RETRYING)).isEmpty());
        assertTrue(JobStateEvaluator.evaluate(List.of(TaskState.FAILED, TaskState.RUNNING)).isEmpty());
    }

    @Test
    void emptyForNullEmptyOrUnresolvedChild() {
        assertTrue(JobStateEvaluator.evaluate(null).isEmpty());
        assertTrue(JobStateEvaluator.evaluate(List.of()).isEmpty());
        // A null element models an unresolved/missing TaskRecord — cannot finalize yet.
        List<TaskState> withNull = new ArrayList<>();
        withNull.add(TaskState.COMPLETED);
        withNull.add(null);
        assertTrue(JobStateEvaluator.evaluate(withNull).isEmpty());
    }

    @Test
    void completedWhenAllCompleted() {
        assertEquals(Optional.of(TaskState.COMPLETED),
                JobStateEvaluator.evaluate(List.of(TaskState.COMPLETED, TaskState.COMPLETED, TaskState.COMPLETED)));
    }

    @Test
    void failedWhenAllFailed() {
        assertEquals(Optional.of(TaskState.FAILED),
                JobStateEvaluator.evaluate(List.of(TaskState.FAILED, TaskState.FAILED)));
    }

    @Test
    void finishedWithErrorsWhenMixedCompletedAndFailed() {
        assertEquals(Optional.of(TaskState.FINISHED_WITH_ERRORS),
                JobStateEvaluator.evaluate(List.of(TaskState.COMPLETED, TaskState.FAILED, TaskState.COMPLETED)));
    }

    @Test
    void cancelledCountsAsTerminalNonCompleted() {
        // All cancelled -> FAILED (no completed task survived).
        assertEquals(Optional.of(TaskState.FAILED),
                JobStateEvaluator.evaluate(List.of(TaskState.CANCELLED, TaskState.CANCELLED)));
        // Mixed completed + cancelled -> partial success.
        assertEquals(Optional.of(TaskState.FINISHED_WITH_ERRORS),
                JobStateEvaluator.evaluate(List.of(TaskState.COMPLETED, TaskState.CANCELLED)));
    }

    // ---- counts-based overload ----

    @Test
    void countsEmptyWhileTasksStillNonTerminal() {
        // terminal < total -> not decided yet.
        assertTrue(JobStateEvaluator.evaluate(3, 1, 0, 0).isEmpty());
        assertTrue(JobStateEvaluator.evaluate(3, 1, 1, 0).isEmpty());
        // No tasks at all.
        assertTrue(JobStateEvaluator.evaluate(0, 0, 0, 0).isEmpty());
    }

    @Test
    void countsCompletedWhenAllCompleted() {
        assertEquals(Optional.of(TaskState.COMPLETED), JobStateEvaluator.evaluate(3, 3, 0, 0));
    }

    @Test
    void countsFailedWhenNoneCompleted() {
        assertEquals(Optional.of(TaskState.FAILED), JobStateEvaluator.evaluate(2, 0, 2, 0));
        // All cancelled also counts as FAILED (no completed survived).
        assertEquals(Optional.of(TaskState.FAILED), JobStateEvaluator.evaluate(2, 0, 0, 2));
    }

    @Test
    void countsFinishedWithErrorsWhenMixed() {
        assertEquals(Optional.of(TaskState.FINISHED_WITH_ERRORS), JobStateEvaluator.evaluate(3, 2, 1, 0));
        assertEquals(Optional.of(TaskState.FINISHED_WITH_ERRORS), JobStateEvaluator.evaluate(2, 1, 0, 1));
    }

    @Test
    void countsAndCollectionOverloadsAgree() {
        // The collection overload must delegate to the same rule.
        assertEquals(
                JobStateEvaluator.evaluate(3, 2, 1, 0),
                JobStateEvaluator.evaluate(List.of(TaskState.COMPLETED, TaskState.COMPLETED, TaskState.FAILED)));
    }
}
