package com.kuaia.engine.coordinator.scheduler;

import com.kuaia.common.model.NodeInfo;
import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.engine.coordinator.registry.WorkerRegistry;
import com.kuaia.engine.coordinator.rpc.StreamManager;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

public class SchedulerTest {
    @Test
    public void testLeastLoadedScheduling() {
        WorkerRegistry registry = new WorkerRegistry();
        NodeInfo w1 = NodeInfo.builder().id("w1").build();
        NodeInfo w2 = NodeInfo.builder().id("w2").build();

        registry.register(w1);
        registry.register(w2);

        registry.updateHeartbeat("w1", 0.8);
        registry.updateHeartbeat("w2", 0.2);

        Scheduler scheduler = new Scheduler(registry);
        Optional<NodeInfo> selected = scheduler.schedule(new TaskDefinition());

        assertTrue(selected.isPresent());
        assertEquals("w2", selected.get().getId());
    }

    @Test
    public void schedulerSkipsPausedWorkersWhenStreamManagerIsPresent() {
        WorkerRegistry registry = new WorkerRegistry();
        NodeInfo w1 = NodeInfo.builder().id("w1").build();
        NodeInfo w2 = NodeInfo.builder().id("w2").build();
        StreamManager streamManager = new StreamManager();
        StreamObserver<CoordinatorMessage> responseObserver = mock(StreamObserver.class);

        registry.register(w1);
        registry.register(w2);
        registry.updateHeartbeat("w1", 0.1);
        registry.updateHeartbeat("w2", 0.9);

        streamManager.registerStream("w1", responseObserver);
        streamManager.registerStream("w2", responseObserver);
        streamManager.setPaused("w1", true);

        Scheduler scheduler = new Scheduler(registry, streamManager);
        Optional<NodeInfo> selected = scheduler.schedule(new TaskDefinition());

        assertTrue(selected.isPresent());
        assertEquals("w2", selected.get().getId());
    }
}
