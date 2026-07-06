package com.kuaia.engine.worker.connector;

import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DocumentTextExtractorTest {
    @TempDir
    Path tempDir;

    @Test
    void rejectsDocumentWithoutTextExtractor() throws Exception {
        Path document = tempDir.resolve("payload.xyz");
        Files.write(document, "binary payload".getBytes(StandardCharsets.UTF_8));

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> DocumentTextExtractor.extractText(Files.readAllBytes(document), "payload.xyz"));

        assertEquals("No text extractor for document: payload.xyz", error.getMessage());
    }
}
