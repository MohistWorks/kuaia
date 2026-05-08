package com.kuaia.engine;

import com.kuaia.engine.coordinator.rpc.StreamManager;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ClusterCommIntegrationTest {
    @Test
    public void testBackpressureStateTracking() {
        StreamManager manager = new StreamManager();
        String workerId = "test-worker";
        StreamObserver responseObserver = mock(StreamObserver.class);

        manager.registerStream(workerId, responseObserver);
        assertTrue(manager.isAvailable(workerId));

        // Simulate high backpressure signal
        manager.setPaused(workerId, true);
        assertFalse(manager.isAvailable(workerId));

        // Simulate low backpressure signal
        manager.setPaused(workerId, false);
        assertTrue(manager.isAvailable(workerId));
    }

    @Test
    public void ackOnlyReregistrationDoesNotClearPausedState() {
        StreamManager manager = new StreamManager();
        String workerId = "test-worker";
        StreamObserver responseObserver = mock(StreamObserver.class);

        manager.registerStream(workerId, responseObserver);
        manager.setPaused(workerId, true);
        manager.registerStream(workerId, responseObserver);

        assertFalse(manager.isAvailable(workerId));
    }
}
