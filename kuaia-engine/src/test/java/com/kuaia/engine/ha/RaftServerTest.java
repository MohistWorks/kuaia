package com.kuaia.engine.ha;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.File;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

public class RaftServerTest {
    @TempDir
    Path tempDir;

    @Test
    public void testRaftStartup() throws Exception {
        RaftServer server = new RaftServer();
        File storageDir = tempDir.toFile();
        // Start a single-node raft cluster for basic test
        server.start("node1", "127.0.0.1:9090", "127.0.0.1:9090", storageDir);
        // If it starts without exception, we consider it a success for this skeleton task
        server.close();
    }
}
