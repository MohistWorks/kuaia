package com.kuaia.engine.worker.connector;

import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public final class DocumentTextExtractor {
    private DocumentTextExtractor() {
    }

    public static String extractText(byte[] bytes, String name) throws PipelineExecutionException {
        if (!isPdf(name) && !isText(name)) {
            throw new PipelineExecutionException("No text extractor for document: " + name);
        }
        try {
            if (isPdf(name)) {
                return extractPdfText(bytes);
            }
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            // pdfbox throws unchecked exceptions (IllegalArgumentException, IndexOutOfBoundsException,
            // NegativeArraySizeException, ...) on malformed PDFs; wrap those too so bad documents stay
            // on the per-record error path instead of killing the whole split.
            throw new PipelineExecutionException(
                    "Document source read failed at " + name + ": " + e.getMessage(),
                    e);
        }
    }

    private static String extractPdfText(byte[] bytes) throws IOException {
        try (PDDocument pdf = Loader.loadPDF(bytes)) {
            PDFTextStripper stripper = new PDFTextStripper();
            // PDFTextStripper defaults both separators to System.lineSeparator(): lineSeparator
            // between the lines of a page and pageEnd after each page. Pin both to "\n" so
            // extracted content and downstream chunk boundaries are identical on every platform.
            stripper.setLineSeparator("\n");
            stripper.setPageEnd("\n");
            return stripper.getText(pdf);
        }
    }

    private static boolean isPdf(String name) {
        return fileName(name).endsWith(".pdf");
    }

    private static boolean isText(String name) {
        String fileName = fileName(name);
        return fileName.endsWith(".txt") || fileName.endsWith(".md") || fileName.endsWith(".markdown");
    }

    private static String fileName(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
