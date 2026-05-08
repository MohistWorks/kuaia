package com.kuaia.engine.coordinator.scheduler;

import com.kuaia.common.model.NodeInfo;
import com.kuaia.common.model.TaskDefinition;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.rpc.StreamManager;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class Scheduler {
    private final WorkerRegistry registry;
    private final StreamManager streamManager;

    public Scheduler(WorkerRegistry registry) {
        this(registry, null);
    }

    public Scheduler(WorkerRegistry registry, StreamManager streamManager) {
        this.registry = registry;
        this.streamManager = streamManager;
    }

    public Optional<NodeInfo> schedule(TaskDefinition task) {
        List<NodeInfo> workers = registry.getAvailableWorkers();
        return workers.stream()
                .filter(worker -> streamManager == null || streamManager.isAvailable(worker.getId()))
                .min(Comparator.comparingDouble(w -> registry.getWorkerLoad(w.getId())));
    }
}
