package com.kuaia.engine.worker.connector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Durable {@code (maxSeqId -> byteLength)} log for a file sink, stored beside the data file as
 * {@code <dataPath>.kuaia-offset}. It lets a resuming sink truncate its output back to the byte
 * length that was committed at the coordinator's resume point, so re-delivered batches overwrite
 * rather than append. Byte length is monotonic in {@code maxSeqId} (a task writes its splits in seq
 * order), so the largest recorded entry at or below the resume point is the correct truncation
 * boundary. Durability is process-crash level (OS page cache): a resume always reconciles the data
 * file to the recorded boundary, so a partially written trailing entry is simply ignored.
 */
public final class FileCommitOffsets {
    private static final String SUFFIX = ".kuaia-offset";

    private final Path sidecar;
    private final List<long[]> entries = new ArrayList<>(); // {maxSeqId, byteLength}

    public FileCommitOffsets(Path dataPath) throws IOException {
        this.sidecar = dataPath.resolveSibling(dataPath.getFileName().toString() + SUFFIX);
        load();
    }

    private void load() throws IOException {
        entries.clear();
        if (!Files.exists(sidecar)) {
            return;
        }
        for (String line : Files.readAllLines(sidecar, StandardCharsets.UTF_8)) {
            int comma = line.indexOf(',');
            if (comma <= 0) {
                continue; // corrupt / half-written line
            }
            try {
                long seq = Long.parseLong(line.substring(0, comma).trim());
                long len = Long.parseLong(line.substring(comma + 1).trim());
                entries.add(new long[] {seq, len});
            } catch (NumberFormatException ignored) {
                // corrupt line — skip it
            }
        }
    }

    /** Append a committed batch boundary. */
    public void record(long maxSeqId, long byteLength) throws IOException {
        entries.add(new long[] {maxSeqId, byteLength});
        Files.writeString(sidecar, maxSeqId + "," + byteLength + "\n", StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    /** Byte length committed at or below {@code resumeFromSeq - 1}; 0 if none. */
    public long truncationPointFor(long resumeFromSeq) {
        long ceiling = resumeFromSeq - 1;
        long best = 0L;
        for (long[] entry : entries) {
            if (entry[0] <= ceiling && entry[1] > best) {
                best = entry[1];
            }
        }
        return best;
    }

    /** Drop entries with {@code maxSeqId > resumeFromSeq - 1} and rewrite the sidecar to match. */
    public void truncateBeyond(long resumeFromSeq) throws IOException {
        long ceiling = resumeFromSeq - 1;
        entries.removeIf(entry -> entry[0] > ceiling);
        rewrite();
    }

    /** Forget all offsets (fresh run). */
    public void reset() throws IOException {
        entries.clear();
        Files.deleteIfExists(sidecar);
    }

    private void rewrite() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (long[] entry : entries) {
            sb.append(entry[0]).append(',').append(entry[1]).append('\n');
        }
        Files.writeString(sidecar, sb.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
