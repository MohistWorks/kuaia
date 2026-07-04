package com.kuaia.engine.worker.connector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileCommitOffsetsTest {

    @Test
    void recordsAndSelectsTruncationPoint(@TempDir Path tmp) throws Exception {
        Path data = tmp.resolve("out.csv");
        FileCommitOffsets offsets = new FileCommitOffsets(data);
        offsets.record(10L, 100L);
        offsets.record(20L, 250L);
        offsets.record(30L, 400L);

        assertEquals(250L, offsets.truncationPointFor(21L)); // last committed <= 20
        assertEquals(400L, offsets.truncationPointFor(31L)); // <= 30
        assertEquals(0L, offsets.truncationPointFor(5L));    // nothing <= 4
    }

    @Test
    void truncateBeyondDropsLaterEntriesAndPersists(@TempDir Path tmp) throws Exception {
        Path data = tmp.resolve("out.csv");
        FileCommitOffsets offsets = new FileCommitOffsets(data);
        offsets.record(10L, 100L);
        offsets.record(20L, 250L);
        offsets.record(30L, 400L);

        offsets.truncateBeyond(21L); // keep <= 20

        FileCommitOffsets reopened = new FileCommitOffsets(data);
        assertEquals(250L, reopened.truncationPointFor(21L));
        assertEquals(250L, reopened.truncationPointFor(1000L));
    }

    @Test
    void ignoresCorruptTrailingLine(@TempDir Path tmp) throws Exception {
        Path data = tmp.resolve("out.csv");
        Path sidecar = tmp.resolve("out.csv.kuaia-offset");
        Files.writeString(sidecar, "10,100\n20,250\n30,4", StandardCharsets.UTF_8); // last line half-written

        FileCommitOffsets offsets = new FileCommitOffsets(data);
        assertEquals(250L, offsets.truncationPointFor(1000L));
    }

    @Test
    void resetClearsSidecar(@TempDir Path tmp) throws Exception {
        Path data = tmp.resolve("out.csv");
        FileCommitOffsets offsets = new FileCommitOffsets(data);
        offsets.record(10L, 100L);
        offsets.reset();
        assertEquals(0L, offsets.truncationPointFor(1000L));
    }
}
