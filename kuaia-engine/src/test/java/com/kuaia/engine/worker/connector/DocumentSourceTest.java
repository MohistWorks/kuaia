package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        assertEquals(
                "Document file is not a supported document: " + document + " (documentType: text)",
                error.getMessage());
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
    void constructorRejectsUnknownDocumentType() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new DocumentSource(tempDir, "html"));

        assertEquals("Unsupported documentType: html", error.getMessage());
    }

    @Test
    void openRejectsMissingPath() {
        Path missing = tempDir.resolve("missing");

        DocumentSource source = new DocumentSource(missing, "auto");

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, source::open);

        assertEquals("Document path not found: " + missing, error.getMessage());
    }

    @Test
    void readFromRejectsSourceThatIsNotOpen() {
        DocumentSource source = new DocumentSource(tempDir, "auto");

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> source.readFrom(
                        0L,
                        (seqId, row) -> {
                            throw new AssertionError("Unexpected row for seq " + seqId);
                        },
                        (seqId, readError) -> {
                            throw new AssertionError("Unexpected row error for seq " + seqId, readError);
                        }));

        assertEquals("Document source is not open", error.getMessage());
    }

    @Test
    void emptyDirectoryErrorIncludesNonAutoDocumentType() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("intro.md"), "Intro document".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(docs, "text");

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, source::open);

        assertEquals(
                "Document directory has no supported documents: " + docs + " (documentType: text)",
                error.getMessage());
    }

    @Test
    void acceptsUppercaseExtensions() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("INTRO.MD"), "Intro document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("NOTES.TXT"), "Notes document".getBytes(StandardCharsets.UTF_8));
        writePdf(docs.resolve("MANUAL.PDF"), "Manual body");

        DocumentSource source = new DocumentSource(docs, "auto");
        source.open();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> rows.add(row),
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(3, read);
        assertEquals("INTRO.MD", rows.get(0).getString(1));
        assertEquals("Intro document", rows.get(0).getString(2));
        assertEquals("MANUAL.PDF", rows.get(1).getString(1));
        assertEquals("Manual body\n", rows.get(1).getString(2));
        assertEquals("NOTES.TXT", rows.get(2).getString(1));
        assertEquals("Notes document", rows.get(2).getString(2));
        source.close();
    }

    @Test
    void singleFileCorpusResumesPastItsOnlyDocument() throws Exception {
        Path document = tempDir.resolve("guide.md");
        Files.write(document, "Guide document".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(document, "auto");
        source.open();

        int read = source.readFrom(
                1L,
                (seqId, row) -> {
                    throw new AssertionError("Unexpected row for seq " + seqId);
                },
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(0, read);
        source.close();
    }

    @Test
    void autoDocumentTypeReadsPdfContentInStableRelativePathOrder() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs.resolve("nested"));
        Files.write(docs.resolve("intro.md"), "Intro document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("nested/guide.txt"), "Guide document".getBytes(StandardCharsets.UTF_8));
        writePdf(docs.resolve("manual.pdf"), "Manual body");

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

        assertEquals(3, read);
        assertEquals(Arrays.asList(1L, 2L, 3L), seqIds);
        assertEquals("intro.md", rows.get(0).getString(1));
        assertEquals("Intro document", rows.get(0).getString(2));
        assertEquals("manual.pdf", rows.get(1).getString(1));
        assertEquals("Manual body\n", rows.get(1).getString(2));
        assertEquals("nested/guide.txt", rows.get(2).getString(1));
        assertEquals("Guide document", rows.get(2).getString(2));
        source.close();
    }

    @Test
    void pdfDocumentTypeSelectsOnlyPdfDocuments() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("intro.md"), "Intro document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("guide.txt"), "Guide document".getBytes(StandardCharsets.UTF_8));
        writePdf(docs.resolve("manual.pdf"), "Manual body");

        DocumentSource source = new DocumentSource(docs, "pdf");
        source.open();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> rows.add(row),
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(1, read);
        assertEquals("manual.pdf", rows.get(0).getString(1));
        assertEquals("Manual body\n", rows.get(0).getString(2));
        source.close();
    }

    @Test
    void textDocumentTypeExcludesPdfDocuments() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("guide.txt"), "Guide document".getBytes(StandardCharsets.UTF_8));
        writePdf(docs.resolve("manual.pdf"), "Manual body");

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
    void corruptPdfIsSkippedWhenErrorConsumerAcceptsIt() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("a.md"), "Alpha document".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("corrupt.pdf"), "not a real pdf".getBytes(StandardCharsets.UTF_8));
        Files.write(docs.resolve("z.txt"), "Zulu document".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(docs, "auto");
        source.open();
        List<BinaryRow> rows = new ArrayList<>();
        List<Long> errorSeqIds = new ArrayList<>();
        List<Exception> errors = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> rows.add(row),
                (seqId, error) -> {
                    errorSeqIds.add(seqId);
                    errors.add(error);
                    return true;
                });

        assertEquals(2, read);
        assertEquals(2, rows.size());
        assertEquals("a.md", rows.get(0).getString(1));
        assertEquals("z.txt", rows.get(1).getString(1));
        assertEquals(Arrays.asList(2L), errorSeqIds);
        assertTrue(
                errors.get(0).getMessage().startsWith("Document source read failed at corrupt.pdf: "),
                "Unexpected error message: " + errors.get(0).getMessage());
        source.close();
    }

    @Test
    void corruptPdfFailsReadWhenErrorConsumerRejectsIt() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        Files.write(docs.resolve("corrupt.pdf"), "not a real pdf".getBytes(StandardCharsets.UTF_8));

        DocumentSource source = new DocumentSource(docs, "auto");
        source.open();

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> source.readFrom(
                        0L,
                        (seqId, row) -> {
                            throw new AssertionError("Unexpected row for seq " + seqId);
                        },
                        (seqId, readError) -> false));

        assertTrue(
                error.getMessage().startsWith("Document source read failed at corrupt.pdf: "),
                "Unexpected error message: " + error.getMessage());
        source.close();
    }

    @Test
    void pdfWithoutTextYieldsRowWithBlankContent() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        writePdf(docs.resolve("blank.pdf"), "");

        DocumentSource source = new DocumentSource(docs, "pdf");
        source.open();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> rows.add(row),
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(1, read);
        assertEquals("blank.pdf", rows.get(0).getString(1));
        assertEquals("\n", rows.get(0).getString(2));
        source.close();
    }

    static void writePdf(Path path, String... pageTexts) throws IOException {
        try (PDDocument document = new PDDocument()) {
            for (String pageText : pageTexts) {
                PDPage page = new PDPage();
                document.addPage(page);
                try (PDPageContentStream contentStream = new PDPageContentStream(document, page)) {
                    if (!pageText.isEmpty()) {
                        contentStream.beginText();
                        contentStream.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
                        contentStream.newLineAtOffset(72, 720);
                        contentStream.showText(pageText);
                        contentStream.endText();
                    }
                }
            }
            document.save(path.toFile());
        }
    }

    @Test
    void multiPagePdfExtractsAllPagesInPageOrder() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        writePdf(docs.resolve("manual.pdf"), "First page body", "Second page body");

        DocumentSource source = new DocumentSource(docs, "pdf");
        source.open();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> rows.add(row),
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(1, read);
        assertEquals("manual.pdf", rows.get(0).getString(1));
        assertEquals("First page body\nSecond page body\n", rows.get(0).getString(2));
        source.close();
    }

    @Test
    void pdfWithNonAsciiLatinTextExtractsAccentedCharacters() throws Exception {
        Path docs = tempDir.resolve("docs");
        Files.createDirectories(docs);
        writePdf(docs.resolve("latin.pdf"), "café résumé");

        DocumentSource source = new DocumentSource(docs, "pdf");
        source.open();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> rows.add(row),
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(1, read);
        assertEquals("latin.pdf", rows.get(0).getString(1));
        assertEquals("café résumé\n", rows.get(0).getString(2));
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
