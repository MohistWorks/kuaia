package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileSinkTest {
    @TempDir
    Path tempDir;

    @Test
    void writesCsvRowsWithQuotedStrings() throws Exception {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "content"},
                new DataType[]{DataType.LONG, DataType.STRING});
        Path output = tempDir.resolve("out/documents.csv");
        FileSink sink = new FileSink(rowType, output, "csv", "overwrite");

        sink.open();
        sink.write(textRow(7L, "Alpha, \"Beta\"\nNext"));
        sink.close();

        String nl = System.lineSeparator();
        assertEquals(
                "id,content" + nl
                        + "7,\"Alpha, \"\"Beta\"\"\nNext\"" + nl,
                new String(Files.readAllBytes(output), StandardCharsets.UTF_8));
    }

    @Test
    void writesJsonlRowsWithEscapedStringsAndVectors() throws Exception {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "content", "embedding"},
                new DataType[]{DataType.LONG, DataType.STRING, DataType.VECTOR});
        Path output = tempDir.resolve("out/documents.jsonl");
        FileSink sink = new FileSink(rowType, output, "jsonl", "overwrite");

        sink.open();
        sink.write(row(7L, "Alpha, \"Beta\"\nNext\tLine", new float[]{1.0f, 2.5f}));
        sink.close();

        assertEquals(
                Collections.singletonList(
                        "{\"id\":7,\"content\":\"Alpha, \\\"Beta\\\"\\nNext\\tLine\",\"embedding\":[1.0,2.5]}"),
                Files.readAllLines(output, StandardCharsets.UTF_8));
    }

    private BinaryRow textRow(long id, String content) {
        BinaryRow row = new BinaryRow(2);
        row.setLong(0, id);
        row.setString(1, content);
        return row;
    }

    private BinaryRow row(long id, String content, float[] embedding) {
        BinaryRow row = new BinaryRow(3);
        row.setLong(0, id);
        row.setString(1, content);
        row.setVector(2, embedding);
        return row;
    }
}
