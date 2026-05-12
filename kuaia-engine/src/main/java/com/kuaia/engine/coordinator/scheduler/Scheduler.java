package com.kuaia.engine.coordinator.scheduler;

import com.kuaia.common.model.NodeInfo;
import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.model.WorkerRecord;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.rpc.StreamManager;
import com.kuaia.engine.coordinator.state.StateStore;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class Scheduler {
    private final WorkerRegistry registry;
    private final StreamManager streamManager;
    private final StateStore stateStore;

    public Scheduler(WorkerRegistry registry) {
        this(registry, null);
    }

    public Scheduler(WorkerRegistry registry, StreamManager streamManager) {
        this(registry, streamManager, null);
    }

    public Scheduler(WorkerRegistry registry, StreamManager streamManager, StateStore stateStore) {
        this.registry = registry;
        this.streamManager = streamManager;
        this.stateStore = stateStore;
    }

    public Optional<NodeInfo> schedule(TaskDefinition task) {
        List<NodeInfo> workers = registry.getAvailableWorkers();
        return workers.stream()
                .filter(worker -> streamManager == null || streamManager.isAvailable(worker.getId()))
                .filter(this::isPersistedWorkerAvailable)
                .min(Comparator.comparingDouble(this::workerLoad));
    }

    private boolean isPersistedWorkerAvailable(NodeInfo worker) {
        if (stateStore == null) {
            return true;
        }
        WorkerRecord record = stateStore.getWorker(worker.getId());
        return record != null
                && record.getState() == WorkerRecord.WorkerState.ONLINE
                && record.isStreamConnected()
                && record.getBackpressureLevel() == WorkerRecord.BackpressureLevel.LOW;
    }

    private double workerLoad(NodeInfo worker) {
        if (stateStore == null) {
            return registry.getWorkerLoad(worker.getId());
        }
        WorkerRecord record = stateStore.getWorker(worker.getId());
        return record == null ? 1.0 : record.getLoadScore();
    }
}
