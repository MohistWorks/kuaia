package com.kuaia.engine.coordinator.scheduler;

import com.kuaia.common.model.NodeInfo;
import com.kuaia.common.model.TaskDefinition;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class Scheduler {
    private final WorkerRegistry registry;

    public Scheduler(WorkerRegistry registry) {
        this.registry = registry;
    }

    public Optional<NodeInfo> schedule(TaskDefinition task) {
        List<NodeInfo> workers = registry.getAvailableWorkers();
        return workers.stream()
                .min(Comparator.comparingDouble(w -> registry.getWorkerLoad(w.getId())));
    }
}
