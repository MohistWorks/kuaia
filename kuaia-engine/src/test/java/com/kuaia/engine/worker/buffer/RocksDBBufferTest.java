package com.kuaia.engine.worker.buffer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

public class RocksDBBufferTest {
    @TempDir
    Path tempDir;

    @Test
    public void testPutAndGet() throws Exception {
        RocksDBBuffer buffer = new RocksDBBuffer();
        buffer.open(tempDir.toString());
        
        byte[] data = "test-data".getBytes();
        buffer.put(100L, data);
        
        assertArrayEquals(data, buffer.get(100L));
        buffer.close();
    }
}
