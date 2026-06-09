package com.kuaia.common.model;

import java.util.Collection;
import java.util.Optional;

/**
 * Pure decision function that aggregates child {@link TaskState}s into a parent Job state,
 * implementing the Task Planner partial-success semantics (design §2.1).
 *
 * <p>The result is empty while any task is still active (or unresolved), meaning the Job has not
 * reached a final verdict yet. Once every task is terminal:
 * <ul>
 *     <li>all {@code COMPLETED} -&gt; {@code COMPLETED}</li>
 *     <li>no {@code COMPLETED} (all failed/cancelled) -&gt; {@code FAILED}</li>
 *     <li>a mix of completed and failed/cancelled -&gt; {@code FINISHED_WITH_ERRORS}</li>
 * </ul>
 *
 * <p>This is intentionally side-effect free so that both the Raft state machine and the in-memory
 * test double can share identical aggregation logic.
 */
public final class JobStateEvaluator {

    private JobStateEvaluator() {
    }

    /** A task is active (not yet finally decided) in any of the pre-terminal states. */
    public static boolean isActive(TaskState state) {
        return state == TaskState.CREATED
                || state == TaskState.DISPATCHING
                || state == TaskState.RUNNING
                || state == TaskState.RETRYING;
    }

    /**
     * @param childStates the resolved states of every task belonging to the job. A {@code null}
     *                    element models an unresolved/missing task record and blocks finalization.
     * @return the finalized job state, or empty if the job is not yet decided.
     */
    public static Optional<TaskState> evaluate(Collection<TaskState> childStates) {
        if (childStates == null || childStates.isEmpty()) {
            return Optional.empty();
        }
        int completed = 0;
        int failed = 0;
        int cancelled = 0;
        for (TaskState state : childStates) {
            if (state == null || isActive(state)) {
                // An active/unresolved child means the job cannot be decided yet; force a mismatch
                // against the total so the counts-based rule returns empty.
                return Optional.empty();
            }
            if (state == TaskState.COMPLETED) {
                completed++;
            } else if (state == TaskState.FAILED) {
                failed++;
            } else if (state == TaskState.CANCELLED) {
                cancelled++;
            }
        }
        return evaluate(childStates.size(), completed, failed, cancelled);
    }

    /**
     * Counts-based equivalent of {@link #evaluate(Collection)}, used for O(1) incremental cascade
     * maintenance where only per-job terminal counters are tracked (not the full child list).
     *
     * @param totalTasks    the number of tasks belonging to the job.
     * @param completedTasks tasks in {@code COMPLETED}.
     * @param failedTasks    tasks in {@code FAILED}.
     * @param cancelledTasks tasks in {@code CANCELLED}.
     * @return the finalized job state, or empty while any task is still non-terminal.
     */
    public static Optional<TaskState> evaluate(int totalTasks, int completedTasks, int failedTasks, int cancelledTasks) {
        if (totalTasks <= 0) {
            return Optional.empty();
        }
        int terminal = completedTasks + failedTasks + cancelledTasks;
        if (terminal < totalTasks) {
            return Optional.empty();
        }
        if (completedTasks == totalTasks) {
            return Optional.of(TaskState.COMPLETED);
        }
        if (completedTasks == 0) {
            return Optional.of(TaskState.FAILED);
        }
        return Optional.of(TaskState.FINISHED_WITH_ERRORS);
    }
}
