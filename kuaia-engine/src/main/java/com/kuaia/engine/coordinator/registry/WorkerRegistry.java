package com.kuaia.engine.coordinator.registry;

import com.kuaia.common.model.NodeInfo;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerRegistry {
    private final Map<String, NodeInfo> workers = new ConcurrentHashMap<>();
    private final Map<String, Long> lastHeartbeat = new ConcurrentHashMap<>();
    private final Map<String, Double> workerLoads = new ConcurrentHashMap<>();

    public void register(NodeInfo node) {
        workers.put(node.getId(), node);
        updateHeartbeat(node.getId(), 0.0);
    }

    public void updateHeartbeat(String workerId, double load) {
        lastHeartbeat.put(workerId, System.currentTimeMillis());
        workerLoads.put(workerId, load);
    }

    public List<NodeInfo> getAvailableWorkers() {
        long timeout = 30000; // 30s
        List<NodeInfo> available = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (String id : workers.keySet()) {
            if (now - lastHeartbeat.get(id) < timeout) {
                available.add(workers.get(id));
            }
        }
        return available;
    }

    public Double getWorkerLoad(String workerId) {
        return workerLoads.getOrDefault(workerId, 1.0);
    }
}
