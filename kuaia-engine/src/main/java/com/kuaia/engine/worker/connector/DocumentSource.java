package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

public class DocumentSource implements LocalSource {
    private static final KuaiaRowType ROW_TYPE = new KuaiaRowType(
            new String[]{"id", "path", "content"},
            new DataType[]{DataType.LONG, DataType.STRING, DataType.STRING});
    private static final Set<String> SUPPORTED_DOCUMENT_TYPES = Set.of("auto", "text", "markdown", "pdf");

    private final Path root;
    private final String documentType;
    private List<Path> documents;

    public DocumentSource(Path root, String documentType) {
        String effectiveDocumentType = documentType == null ? "auto" : documentType;
        if (!SUPPORTED_DOCUMENT_TYPES.contains(effectiveDocumentType)) {
            throw new IllegalArgumentException("Unsupported documentType: " + documentType);
        }
        this.root = root;
        this.documentType = effectiveDocumentType;
    }

    @Override
    public void open() throws PipelineExecutionException {
        if (!Files.exists(root)) {
            throw new PipelineExecutionException("Document path not found: " + root);
        }
        if (Files.isRegularFile(root)) {
            if (!isSupportedDocument(root)) {
                throw new PipelineExecutionException(
                        "Document file is not a supported document: " + root + documentTypeSuffix());
            }
            documents = new ArrayList<>();
            documents.add(root);
            return;
        }
        if (!Files.isDirectory(root)) {
            throw new PipelineExecutionException("Document source path is not a directory: " + root);
        }
        try (Stream<Path> stream = Files.walk(root)) {
            documents = new ArrayList<>();
            stream.filter(Files::isRegularFile)
                    .filter(this::isSupportedDocument)
                    .sorted(Comparator.comparing(this::relativePath))
                    .forEach(documents::add);
        } catch (IOException e) {
            throw new PipelineExecutionException("Document directory scan failed: " + root + ": " + e.getMessage(), e);
        }
        if (documents.isEmpty()) {
            throw new PipelineExecutionException(
                    "Document directory has no supported documents: " + root + documentTypeSuffix());
        }
    }

    @Override
    public int readFrom(long lastCheckpointSeq, RecordConsumer consumer, RecordErrorConsumer errorConsumer)
            throws Exception {
        ensureOpen();
        int count = 0;
        for (int i = 0; i < documents.size(); i++) {
            long seqId = i + 1L;
            if (seqId <= lastCheckpointSeq) {
                continue;
            }
            Path document = documents.get(i);
            BinaryRow row;
            try {
                row = readDocument(seqId, document);
            } catch (PipelineExecutionException e) {
                if (errorConsumer.accept(seqId, e)) {
                    continue;
                }
                throw e;
            }
            consumer.accept(seqId, row);
            count++;
        }
        return count;
    }

    @Override
    public KuaiaRowType getRowType() {
        return ROW_TYPE;
    }

    @Override
    public void close() {
        documents = null;
    }

    private BinaryRow readDocument(long seqId, Path document) throws PipelineExecutionException {
        String content = DocumentTextExtractor.extractText(document, relativePath(document));
        BinaryRow row = new BinaryRow(3);
        row.setLong(0, seqId);
        row.setString(1, relativePath(document));
        row.setString(2, content);
        return row;
    }

    private boolean isSupportedDocument(Path path) {
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        if ("text".equals(documentType)) {
            return name.endsWith(".txt");
        }
        if ("markdown".equals(documentType)) {
            return name.endsWith(".md") || name.endsWith(".markdown");
        }
        if ("pdf".equals(documentType)) {
            return name.endsWith(".pdf");
        }
        return name.endsWith(".txt") || name.endsWith(".md") || name.endsWith(".markdown") || name.endsWith(".pdf");
    }

    private String documentTypeSuffix() {
        if ("auto".equals(documentType)) {
            return "";
        }
        return " (documentType: " + documentType + ")";
    }

    private String relativePath(Path path) {
        if (path.equals(root)) {
            return path.getFileName().toString();
        }
        return root.relativize(path).toString().replace('\\', '/');
    }

    private void ensureOpen() throws PipelineExecutionException {
        if (documents == null) {
            throw new PipelineExecutionException("Document source is not open");
        }
    }
}
