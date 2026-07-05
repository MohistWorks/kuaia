package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DocumentSourceTest {
    @TempDir
    Path tempDir;

    @Test
    void readsSupportedDocumentsInStableRelativePathOrder() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs.resolve("nested"));
        Files.write(docs.resolve("intro.md"), "Intro document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("nested/guide.txt"), "Guide document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("image.png"), "ignored".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(docs, "auto");
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
        assertArrayEquals(new String[]{"id", "path", "content"}, source.getRowType().getFieldNames());
        assertArrayEquals(
                new DataType[]{DataType.LONG, DataType.STRING, DataType.STRING},
                source.getRowType().getFieldTypes());
        assertEquals(Arrays.asList(1L, 2L), seqIds);
        assertEquals(1L, rows.get(0).getLong(0));
        assertEquals("intro.md", rows.get(0).getString(1));
        assertEquals("Intro document", rows.get(0).getString(2));
        assertEquals(2L, rows.get(1).getLong(0));
        assertEquals("nested/guide.txt", rows.get(1).getString(1));
        assertEquals("Guide document", rows.get(1).getString(2));
        source.close();
    }

    @Test
    void rejectsDirectoryWithoutSupportedDocuments() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("image.png"), "ignored".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(docs, "auto");

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, source::open);

        assertEquals("Document directory has no supported documents: " + docs, error.getMessage());
    }

    @Test
    void readsSingleRegularFileAsOneDocumentCorpus() throws Exception {
        Path document = tempDir.resolve("guide.md");
        Files.write(document, "Guide document".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(document, "auto");
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

        assertEquals(1, read);
        assertEquals(Arrays.asList(1L), seqIds);
        assertEquals(1L, rows.get(0).getLong(0));
        assertEquals("guide.md", rows.get(0).getString(1));
        assertEquals("Guide document", rows.get(0).getString(2));
        source.close();
    }

    @Test
    void rejectsSingleRegularFileWithUnsupportedExtension() throws Exception {
        Path document = tempDir.resolve("image.png");
        Files.write(document, "ignored".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(document, "auto");

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, source::open);

        assertEquals("Document file is not a supported document: " + document, error.getMessage());
    }

    @Test
    void rejectsSingleRegularFileNotMatchingDocumentTypeFilter() throws Exception {
        Path document = tempDir.resolve("guide.md");
        Files.write(document, "Guide document".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(document, "text");

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, source::open);

        assertEquals("Document file is not a supported document: " + document, error.getMessage());
    }

    @Test
    void textDocumentTypeExcludesMarkdownDocuments() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("intro.md"), "Intro document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("notes.markdown"), "Notes document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("guide.txt"), "Guide document".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(docs, "text");
        source.open();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> rows.add(row),
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(1, read);
        assertEquals("guide.txt", rows.get(0).getString(1));
        source.close();
    }

    @Test
    void markdownDocumentTypeExcludesTextDocuments() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("intro.md"), "Intro document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("notes.markdown"), "Notes document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("guide.txt"), "Guide document".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(docs, "markdown");
        source.open();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> rows.add(row),
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(2, read);
        assertEquals("intro.md", rows.get(0).getString(1));
        assertEquals("notes.markdown", rows.get(1).getString(1));
        source.close();
    }

    @Test
    void nullDocumentTypeBehavesLikeAuto() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("intro.md"), "Intro document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("guide.txt"), "Guide document".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(docs, null);
        source.open();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> rows.add(row),
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(2, read);
        assertEquals("guide.txt", rows.get(0).getString(1));
        assertEquals("intro.md", rows.get(1).getString(1));
        source.close();
    }
}
