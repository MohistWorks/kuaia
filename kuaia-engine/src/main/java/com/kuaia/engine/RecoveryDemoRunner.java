package com.kuaia.engine;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.engine.coordinator.recovery.CoordinatorRecoveryPlanner;
import com.kuaia.engine.coordinator.state.RocksDbStateStore;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public class RecoveryDemoRunner {
    public List<TaskRecord> run(Path stateDir, PrintStream out) throws Exception {
        TaskRecord running = TaskRecord.created("job-demo", "task-recovering")
                .dispatching("worker-1", "attempt-1", 50L)
                .running()
                .checkpoint("attempt-1", 7L);

        try (RocksDbStateStore store = new RocksDbStateStore(stateDir)) {
            store.saveTask(running);
        }

        try (RocksDbStateStore reopened = new RocksDbStateStore(stateDir)) {
            List<TaskRecord> recovered = new CoordinatorRecoveryPlanner(reopened)
                    .recoverSchedulableTasks(100L);
            out.println("Recovered schedulable tasks: " + taskIds(recovered));
            for (TaskRecord record : recovered) {
                out.println(record.getTaskId()
                        + " state=" + record.getState()
                        + " checkpoint=" + record.getLastCheckpointSeq());
            }
            return recovered;
        }
    }

    private String taskIds(List<TaskRecord> records) {
        return records.stream()
                .map(TaskRecord::getTaskId)
                .collect(Collectors.joining(","));
    }
}
