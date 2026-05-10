package com.kuaia.engine.worker.connector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileSourceTest {
    @TempDir
    Path tempDir;

    @Test
    void rangeReadStopsBeforeUpperBoundWithoutParsingLaterRows() throws Exception {
        Path csv = tempDir.resolve("documents.csv");
        Files.write(csv, String.join("\n",
                "id,content",
                "1,Alpha",
                "2,Beta",
                "bad,Gamma").getBytes(StandardCharsets.UTF_8));
        FileSource source = new FileSource(csv);
        source.open();
        List<Long> seqIds = new ArrayList<>();

        int read = source.readRange(
                0L,
                2L,
                (seqId, row) -> seqIds.add(seqId),
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(2, read);
        assertEquals(Arrays.asList(1L, 2L), seqIds);
        source.close();
    }
}
