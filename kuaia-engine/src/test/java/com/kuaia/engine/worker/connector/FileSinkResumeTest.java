package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileSinkResumeTest {

    static final KuaiaRowType ROW_TYPE =
            new KuaiaRowType(new String[] {"id"}, new DataType[] {DataType.LONG});

    static BinaryRow rowOf(long id) {
        BinaryRow row = new BinaryRow(1);
        row.setLong(0, id);
        return row;
    }

    @Test
    void resumeTruncatesToCommittedPointAndAppendsWithoutDuplication(@TempDir Path tmp) throws Exception {
        Path out = tmp.resolve("out.csv");

        // First attempt: write rows for seq 1,2 (commit @2), then 3,4 (commit @4).
        FileSink first = new FileSink(ROW_TYPE, out, "csv", "overwrite", 1L);
        first.open();
        first.write(rowOf(1));
        first.write(rowOf(2));
        first.recordCommit(2L);
        first.write(rowOf(3));
        first.write(rowOf(4));
        first.recordCommit(4L);
        first.close();

        // Coordinator only durably checkpointed seq 2; the retry resumes from seq 3.
        FileSink retry = new FileSink(ROW_TYPE, out, "csv", "overwrite", 3L);
        retry.open();                 // must truncate the file back to the seq-2 boundary
        retry.write(rowOf(3));
        retry.write(rowOf(4));
        retry.recordCommit(4L);
        retry.close();

        // header + rows 1,2,3,4 exactly once — no duplicate 3/4 from the first attempt.
        assertEquals(List.of("id", "1", "2", "3", "4"), Files.readAllLines(out, StandardCharsets.UTF_8));
    }

    @Test
    void freshRunTruncatesPreviousContent(@TempDir Path tmp) throws Exception {
        Path out = tmp.resolve("out.csv");
        Files.writeString(out, "stale\n", StandardCharsets.UTF_8);

        FileSink sink = new FileSink(ROW_TYPE, out, "csv", "overwrite", 1L);
        sink.open();
        sink.write(rowOf(7));
        sink.recordCommit(7L);
        sink.close();

        assertEquals(List.of("id", "7"), Files.readAllLines(out, StandardCharsets.UTF_8));
    }
}
