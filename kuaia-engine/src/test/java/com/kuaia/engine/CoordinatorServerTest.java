package com.kuaia.engine;

import com.kuaia.engine.coordinator.state.InMemoryStateStore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorServerTest {

    @Test
    void startBindsEphemeralPortAndCloseIsIdempotent() throws Exception {
        CoordinatorServer server = new CoordinatorServer(new InMemoryStateStore(), 30_000L);
        server.start(0);
        assertTrue(server.port() > 0, "ephemeral port should be assigned");
        server.close();
        server.close(); // second close must be a no-op, not throw
    }
}
