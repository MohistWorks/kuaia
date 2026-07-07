package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.pipeline.PipelineExecutionException;
import com.kuaia.common.type.DataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileSourceTest {
    @TempDir
    Path tempDir;

    @Test
    void csvReadsQuotedFieldsWithCommasQuotesAndNewlines() throws Exception {
        Path csv = tempDir.resolve("quoted-documents.csv");
        Files.write(csv, String.join("\n",
                "id,content",
                "1,\"Alpha, \"\"Beta\"\"",
                "Next\"",
                "2,Gamma").getBytes(StandardCharsets.UTF_8));
        FileSource source = new FileSource(csv);
        source.open();
        List<Long> seqIds = new ArrayList<>();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> {
                    seqIds.add(seqId);
                    rows.add(row);
                },
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(2, read);
        assertEquals(2L, source.getRecordCount());
        assertEquals(Arrays.asList(1L, 2L), seqIds);
        assertEquals(1L, rows.get(0).getLong(0));
        assertEquals("Alpha, \"Beta\"\nNext", rows.get(0).getString(1));
        assertEquals(2L, rows.get(1).getLong(0));
        assertEquals("Gamma", rows.get(1).getString(1));
        source.close();
    }

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

    @Test
    void jsonlInfersSchemaAndReadsRange() throws Exception {
        Path jsonl = tempDir.resolve("documents.jsonl");
        Files.write(jsonl, String.join("\n",
                "",
                "{\"id\":1,\"content\":\"Alpha\",\"category\":\"doc\"}",
                "{\"id\":2,\"content\":\"Beta\",\"category\":\"doc\"}",
                "{\"id\":3,\"content\":\"Gamma\",\"category\":\"note\"}").getBytes(StandardCharsets.UTF_8));
        FileSource source = new FileSource(jsonl, "jsonl");
        source.open();
        List<Long> seqIds = new ArrayList<>();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readRange(
                1L,
                2L,
                (seqId, row) -> {
                    seqIds.add(seqId);
                    rows.add(row);
                },
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(1, read);
        assertEquals(3L, source.getRecordCount());
        assertArrayEquals(new String[]{"id", "content", "category"}, source.getRowType().getFieldNames());
        assertArrayEquals(
                new DataType[]{DataType.LONG, DataType.STRING, DataType.STRING},
                source.getRowType().getFieldTypes());
        assertEquals(Arrays.asList(2L), seqIds);
        assertEquals(2L, rows.get(0).getLong(0));
        assertEquals("Beta", rows.get(0).getString(1));
        assertEquals("doc", rows.get(0).getString(2));
        source.close();
    }

    @Test
    void jsonlBadRecordCanBeSkipped() throws Exception {
        Path jsonl = tempDir.resolve("bad-documents.jsonl");
        Files.write(jsonl, String.join("\n",
                "{\"id\":1,\"content\":\"Alpha\"}",
                "{\"id\":2,\"content\":\"Beta\"",
                "{\"id\":3,\"content\":\"Gamma\"}").getBytes(StandardCharsets.UTF_8));
        FileSource source = new FileSource(jsonl, "jsonl");
        source.open();
        List<Long> seqIds = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> seqIds.add(seqId),
                (seqId, error) -> {
                    errors.add(seqId + ":" + error.getMessage());
                    return true;
                });

        assertEquals(2, read);
        assertEquals(Arrays.asList(1L, 3L), seqIds);
        assertEquals(1, errors.size());
        assertEquals("2:Invalid JSONL row at line 2: malformed JSON", errors.get(0));
        source.close();
    }

    @Test
    void jsonlDecodesMalformedUtf8AsReplacementCharAndStillParses() throws Exception {
        // 0xFF is never a valid UTF-8 byte. The JSONL reader decodes with REPLACE (the InputStreamReader
        // default), so it becomes the U+FFFD replacement char instead of throwing the way
        // Files.readAllLines (REPORT) would. This pins that intentional leniency, which matches the CSV
        // path. A future change to this behavior should be a conscious decision.
        byte[] schemaLine = "{\"id\":1,\"content\":\"seed\"}\n".getBytes(StandardCharsets.UTF_8);
        byte[] rowPrefix = "{\"id\":2,\"content\":\"a".getBytes(StandardCharsets.UTF_8);
        byte[] rowSuffix = "b\"}".getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.writeBytes(schemaLine);
        buffer.writeBytes(rowPrefix);
        buffer.write(0xFF);
        buffer.writeBytes(rowSuffix);
        Path jsonl = tempDir.resolve("malformed.jsonl");
        Files.write(jsonl, buffer.toByteArray());
        FileSource source = new FileSource(jsonl, "jsonl");
        source.open();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> rows.add(row),
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(2, read);
        assertEquals("seed", rows.get(0).getString(1));
        assertEquals("a�b", rows.get(1).getString(1));
        source.close();
    }

    @Test
    void jsonlRejectsNestedValues() throws Exception {
        Path jsonl = tempDir.resolve("nested-documents.jsonl");
        Files.write(jsonl, String.join("\n",
                "{\"id\":1,\"content\":\"Alpha\"}",
                "{\"id\":2,\"content\":{\"text\":\"Beta\"}}").getBytes(StandardCharsets.UTF_8));
        FileSource source = new FileSource(jsonl, "jsonl");
        source.open();

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> source.readFrom(0L, (seqId, row) -> {}, (seqId, rowError) -> false));

        assertEquals("Invalid JSONL row at line 2: field content must be a scalar value", error.getMessage());
        source.close();
    }
}
