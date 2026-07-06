package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class DocumentSource implements LocalSource {
    private static final KuaiaRowType ROW_TYPE = new KuaiaRowType(
            new String[]{"id", "path", "content"},
            new DataType[]{DataType.LONG, DataType.STRING, DataType.STRING});
    private static final Set<String> SUPPORTED_DOCUMENT_TYPES = Set.of("auto", "text", "markdown", "pdf");

    private final KuaiaFileSystem fs;
    private final String rootUri;
    private final String documentType;
    private List<String> documentUris;

    public DocumentSource(Path root, String documentType) {
        this(new LocalFileSystem(), root.toString(), documentType);
    }

    public DocumentSource(KuaiaFileSystem fs, String rootUri, String documentType) {
        String effectiveDocumentType = documentType == null ? "auto" : documentType;
        if (!SUPPORTED_DOCUMENT_TYPES.contains(effectiveDocumentType)) {
            throw new IllegalArgumentException("Unsupported documentType: " + documentType);
        }
        this.fs = fs;
        this.rootUri = rootUri;
        this.documentType = effectiveDocumentType;
    }

    @Override
    public void open() throws PipelineExecutionException {
        if (!fs.exists(rootUri)) {
            throw new PipelineExecutionException("Document path not found: " + rootUri);
        }
        if (!fs.isDirectory(rootUri)) {
            if (!isSupportedDocument(fileName(rootUri))) {
                throw new PipelineExecutionException(
                        "Document file is not a supported document: " + rootUri + documentTypeSuffix());
            }
            documentUris = new ArrayList<>();
            documentUris.add(rootUri);
            return;
        }
        List<String> children;
        try {
            children = fs.list(rootUri);
        } catch (UncheckedIOException e) {
            throw new PipelineExecutionException(
                    "Document directory scan failed: " + rootUri + ": " + e.getCause().getMessage(), e);
        }
        documentUris = new ArrayList<>();
        children.stream()
                .filter(uri -> isSupportedDocument(fileName(uri)))
                .sorted(Comparator.comparing(this::relativePath))
                .forEach(documentUris::add);
        if (documentUris.isEmpty()) {
            throw new PipelineExecutionException(
                    "Document directory has no supported documents: " + rootUri + documentTypeSuffix());
        }
    }

    @Override
    public int readFrom(long lastCheckpointSeq, RecordConsumer consumer, RecordErrorConsumer errorConsumer)
            throws Exception {
        ensureOpen();
        int count = 0;
        for (int i = 0; i < documentUris.size(); i++) {
            long seqId = i + 1L;
            if (seqId <= lastCheckpointSeq) {
                continue;
            }
            String document = documentUris.get(i);
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
        documentUris = null;
    }

    private BinaryRow readDocument(long seqId, String documentUri) throws PipelineExecutionException {
        String display = relativePath(documentUri);
        byte[] bytes;
        try {
            bytes = fs.readAllBytes(documentUri);
        } catch (Exception e) {
            throw new PipelineExecutionException(
                    "Document source read failed at " + display + ": " + e.getMessage(), e);
        }
        String content = DocumentTextExtractor.extractText(bytes, display);
        BinaryRow row = new BinaryRow(3);
        row.setLong(0, seqId);
        row.setString(1, display);
        row.setString(2, content);
        return row;
    }

    private boolean isSupportedDocument(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        if ("text".equals(documentType)) {
            return lower.endsWith(".txt");
        }
        if ("markdown".equals(documentType)) {
            return lower.endsWith(".md") || lower.endsWith(".markdown");
        }
        if ("pdf".equals(documentType)) {
            return lower.endsWith(".pdf");
        }
        return lower.endsWith(".txt") || lower.endsWith(".md") || lower.endsWith(".markdown") || lower.endsWith(".pdf");
    }

    private String documentTypeSuffix() {
        if ("auto".equals(documentType)) {
            return "";
        }
        return " (documentType: " + documentType + ")";
    }

    /**
     * The {@code path} column / sort key. Mirrors the old {@code Path}-based behavior: single-file
     * corpus (the object equals the root) uses the file name; directory members use the child URI
     * relative to the root, with {@code \\} normalized to {@code /}.
     */
    private String relativePath(String uri) {
        if (uri.equals(rootUri)) {
            return fileName(rootUri);
        }
        String rest = uri.substring(rootUri.length());
        int i = 0;
        while (i < rest.length() && (rest.charAt(i) == '/' || rest.charAt(i) == '\\')) {
            i++;
        }
        return rest.substring(i).replace('\\', '/');
    }

    private static String fileName(String uri) {
        String normalized = uri.replace('\\', '/');
        int slash = normalized.lastIndexOf('/');
        return slash < 0 ? normalized : normalized.substring(slash + 1);
    }

    private void ensureOpen() throws PipelineExecutionException {
        if (documentUris == null) {
            throw new PipelineExecutionException("Document source is not open");
        }
    }
}
