package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

public class DocumentDirectorySource implements LocalSource {
    private static final KuaiaRowType ROW_TYPE = new KuaiaRowType(
            new String[]{"id", "path", "content"},
            new DataType[]{DataType.LONG, DataType.STRING, DataType.STRING});

    private final Path root;
    private List<Path> documents;

    public DocumentDirectorySource(Path root) {
        this.root = root;
    }

    @Override
    public void open() throws PipelineExecutionException {
        if (!Files.exists(root)) {
            throw new PipelineExecutionException("Document directory not found: " + root);
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
            throw new PipelineExecutionException("Document directory has no supported documents: " + root);
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
        String content;
        try {
            content = new String(Files.readAllBytes(document), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new PipelineExecutionException(
                    "Document source read failed at " + relativePath(document) + ": " + e.getMessage(),
                    e);
        }
        BinaryRow row = new BinaryRow(3);
        row.setLong(0, seqId);
        row.setString(1, relativePath(document));
        row.setString(2, content);
        return row;
    }

    private boolean isSupportedDocument(Path path) {
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        return name.endsWith(".txt") || name.endsWith(".md") || name.endsWith(".markdown");
    }

    private String relativePath(Path path) {
        return root.relativize(path).toString().replace('\\', '/');
    }

    private void ensureOpen() throws PipelineExecutionException {
        if (documents == null) {
            throw new PipelineExecutionException("Document directory source is not open");
        }
    }
}
