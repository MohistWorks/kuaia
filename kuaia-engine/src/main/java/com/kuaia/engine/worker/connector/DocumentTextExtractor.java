package com.kuaia.engine.worker.connector;

import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

public final class DocumentTextExtractor {
    private DocumentTextExtractor() {
    }

    public static String extractText(Path document, String relativePath) throws PipelineExecutionException {
        try {
            if (isPdf(document)) {
                return extractPdfText(document);
            }
            return new String(Files.readAllBytes(document), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new PipelineExecutionException(
                    "Document source read failed at " + relativePath + ": " + e.getMessage(),
                    e);
        }
    }

    private static String extractPdfText(Path document) throws IOException {
        try (PDDocument pdf = Loader.loadPDF(document.toFile())) {
            return new PDFTextStripper().getText(pdf);
        }
    }

    private static boolean isPdf(Path document) {
        return document.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".pdf");
    }
}
