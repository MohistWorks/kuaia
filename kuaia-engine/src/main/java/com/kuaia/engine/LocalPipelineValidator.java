package com.kuaia.engine;

import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.pipeline.embedding.EmbeddingProviderRegistry;
import com.kuaia.engine.pipeline.transform.TransformPipeline;
import com.kuaia.engine.worker.connector.DocumentSource;
import com.kuaia.engine.worker.connector.FileSource;

import java.io.PrintStream;
import java.nio.file.Paths;

public class LocalPipelineValidator {
    private static final String SOURCE_STAGE = "Source";
    private static final String TRANSFORM_STAGE = "Transform";
    private static final String SINK_STAGE = "Sink";

    private final EmbeddingProviderRegistry embeddingProviders;

    public LocalPipelineValidator() {
        this(EmbeddingProviderRegistry.defaultRegistry());
    }

    LocalPipelineValidator(EmbeddingProviderRegistry embeddingProviders) {
        this.embeddingProviders = embeddingProviders;
    }

    public void validate(PipelineConfig config, PrintStream out) throws Exception {
        if (isDeferredSource(config.getSource().getType())) {
            printDeferredReport(config, out);
            return;
        }

        KuaiaRowType sourceType = runStage(SOURCE_STAGE, () -> loadSourceType(config));
        TransformPipeline transforms = runStage(TRANSFORM_STAGE, () -> TransformPipeline.from(
                sourceType,
                config.getTransforms(),
                embeddingProviders));
        runStage(SINK_STAGE, () -> {
            validateSink(config, transforms.getOutputType());
            return null;
        });

        out.println("Pipeline valid: " + config.getName());
        out.println("Source: " + config.getSource().getType() + " fields=" + sourceType.getFieldNames().length);
        out.println("Transforms: " + config.getTransforms().size());
        out.println("Sink: " + config.getSink().getType());
    }

    private void printDeferredReport(PipelineConfig config, PrintStream out) {
        out.println("Pipeline valid: " + config.getName());
        out.println("Source: " + config.getSource().getType() + " fields=deferred");
        out.println("Transforms: " + config.getTransforms().size());
        out.println("Sink: " + config.getSink().getType());
        out.println("Transform and sink row-type checks deferred for source.type: " + config.getSource().getType());
    }

    private boolean isDeferredSource(String sourceType) {
        return "postgres".equals(sourceType)
                || "mysql".equals(sourceType)
                || "duckdb".equals(sourceType)
                || "s3".equals(sourceType);
    }

    private KuaiaRowType loadSourceType(PipelineConfig config) throws Exception {
        if ("file".equals(config.getSource().getType())) {
            if ("document".equals(config.getSource().getFormat())) {
                DocumentSource source = new DocumentSource(
                        Paths.get(config.getSource().getPath()),
                        config.getSource().getDocumentType());
                try {
                    source.open();
                    return source.getRowType();
                } finally {
                    source.close();
                }
            }
            FileSource source = new FileSource(
                    Paths.get(config.getSource().getPath()),
                    config.getSource().getFormat());
            try {
                source.open();
                return source.getRowType();
            } finally {
                source.close();
            }
        }
        throw new PipelineExecutionException("Unsupported source.type: " + config.getSource().getType());
    }

    private void validateSink(PipelineConfig config, KuaiaRowType rowType) throws PipelineExecutionException {
        String sinkType = config.getSink().getType();
        if ("console".equals(sinkType)) {
            return;
        }
        if ("file".equals(sinkType)) {
            validateFileSink(rowType);
            return;
        }
        if ("mock-vector".equals(sinkType)) {
            requireField(rowType, "id", DataType.LONG, "Mock vector sink");
            requireField(rowType, "embedding", DataType.VECTOR, "Mock vector sink");
            return;
        }
        if ("qdrant".equals(sinkType)) {
            validateQdrantSink(config, rowType);
            return;
        }
        if ("pgvector".equals(sinkType)) {
            validatePgvectorSink(config, rowType);
            return;
        }
        if ("milvus".equals(sinkType)) {
            validateMilvusSink(config, rowType);
            return;
        }
        throw new PipelineExecutionException("Unsupported sink.type: " + sinkType);
    }

    private void validateFileSink(KuaiaRowType rowType) throws PipelineExecutionException {
        for (DataType type : rowType.getFieldTypes()) {
            if (type != DataType.LONG && type != DataType.STRING && type != DataType.VECTOR) {
                throw new PipelineExecutionException("File sink does not support field type: " + type.name());
            }
        }
    }

    private void validateQdrantSink(PipelineConfig config, KuaiaRowType rowType) throws PipelineExecutionException {
        int vectorOrdinal = requireField(
                rowType,
                config.getSink().getVectorField(),
                DataType.VECTOR,
                "Qdrant sink");
        requireField(rowType, config.getSink().getIdField(), DataType.LONG, "Qdrant sink");
        if (config.getSink().getChunkIndexField() != null) {
            requireField(rowType, config.getSink().getChunkIndexField(), DataType.LONG, "Qdrant sink");
        }
        for (String payloadField : config.getSink().getPayloadFields()) {
            int ordinal = rowType.getIndex(payloadField);
            if (ordinal < 0) {
                throw new PipelineExecutionException("Qdrant sink requires payload field: " + payloadField);
            }
            if (ordinal == vectorOrdinal) {
                throw new PipelineExecutionException("Qdrant payload field must not be the vector field: " + payloadField);
            }
            DataType type = rowType.getFieldTypes()[ordinal];
            if (type != DataType.LONG && type != DataType.STRING) {
                throw new PipelineExecutionException("Qdrant sink does not support payload field type: " + type.name());
            }
        }
    }

    private void validatePgvectorSink(PipelineConfig config, KuaiaRowType rowType) throws PipelineExecutionException {
        int vectorOrdinal = requireField(
                rowType,
                config.getSink().getVectorField(),
                DataType.VECTOR,
                "Pgvector sink");
        int idOrdinal = requireField(rowType, config.getSink().getIdField(), DataType.LONG, "Pgvector sink");
        if (config.getSink().getPayloadFields().isEmpty()) {
            for (int i = 0; i < rowType.getFieldNames().length; i++) {
                if (i != idOrdinal && i != vectorOrdinal) {
                    validatePgvectorPayloadField(rowType, i);
                }
            }
            return;
        }
        for (String payloadField : config.getSink().getPayloadFields()) {
            int ordinal = rowType.getIndex(payloadField);
            if (ordinal < 0) {
                throw new PipelineExecutionException("Pgvector sink requires payload field: " + payloadField);
            }
            if (ordinal == idOrdinal) {
                throw new PipelineExecutionException("Pgvector payload field must not be the id field: " + payloadField);
            }
            if (ordinal == vectorOrdinal) {
                throw new PipelineExecutionException(
                        "Pgvector payload field must not be the vector field: " + payloadField);
            }
            validatePgvectorPayloadField(rowType, ordinal);
        }
    }

    private void validatePgvectorPayloadField(KuaiaRowType rowType, int ordinal) throws PipelineExecutionException {
        DataType type = rowType.getFieldTypes()[ordinal];
        if (type != DataType.LONG && type != DataType.STRING) {
            throw new PipelineExecutionException(
                    "Pgvector sink does not support payload field type: " + type.name());
        }
    }

    private void validateMilvusSink(PipelineConfig config, KuaiaRowType rowType) throws PipelineExecutionException {
        int vectorOrdinal = requireField(
                rowType,
                config.getSink().getVectorField(),
                DataType.VECTOR,
                "Milvus sink");
        int idOrdinal = requireField(rowType, config.getSink().getIdField(), DataType.LONG, "Milvus sink");
        if (config.getSink().getPayloadFields().isEmpty()) {
            for (int i = 0; i < rowType.getFieldNames().length; i++) {
                if (i != idOrdinal && i != vectorOrdinal) {
                    validateMilvusPayloadField(rowType, i);
                }
            }
            return;
        }
        for (String payloadField : config.getSink().getPayloadFields()) {
            int ordinal = rowType.getIndex(payloadField);
            if (ordinal < 0) {
                throw new PipelineExecutionException("Milvus sink requires payload field: " + payloadField);
            }
            if (ordinal == idOrdinal) {
                throw new PipelineExecutionException("Milvus payload field must not be the id field: " + payloadField);
            }
            if (ordinal == vectorOrdinal) {
                throw new PipelineExecutionException(
                        "Milvus payload field must not be the vector field: " + payloadField);
            }
            validateMilvusPayloadField(rowType, ordinal);
        }
    }

    private void validateMilvusPayloadField(KuaiaRowType rowType, int ordinal) throws PipelineExecutionException {
        DataType type = rowType.getFieldTypes()[ordinal];
        if (type != DataType.LONG && type != DataType.STRING) {
            throw new PipelineExecutionException(
                    "Milvus sink does not support payload field type: " + type.name());
        }
    }

    private int requireField(KuaiaRowType rowType, String field, DataType type, String owner)
            throws PipelineExecutionException {
        int ordinal = rowType.getIndex(field);
        if (ordinal < 0 || rowType.getFieldTypes()[ordinal] != type) {
            throw new PipelineExecutionException(owner + " requires " + type.name() + " field: " + field);
        }
        return ordinal;
    }

    private <T> T runStage(String stage, StageOperation<T> operation) throws Exception {
        try {
            return operation.run();
        } catch (PipelineExecutionException e) {
            if (isStageFailure(e)) {
                throw e;
            }
            throw new PipelineExecutionException(stage + " stage failed: " + e.getMessage(), e);
        } catch (Exception e) {
            if (isStageFailure(e)) {
                throw e;
            }
            throw new PipelineExecutionException(stage + " stage failed: " + errorMessage(e), e);
        }
    }

    private boolean isStageFailure(Exception error) {
        String message = error.getMessage();
        return message != null
                && (message.startsWith(SOURCE_STAGE + " stage failed:")
                || message.startsWith(TRANSFORM_STAGE + " stage failed:")
                || message.startsWith(SINK_STAGE + " stage failed:"));
    }

    private String errorMessage(Exception error) {
        String message = error.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return error.getClass().getSimpleName();
        }
        return message;
    }

    private interface StageOperation<T> {
        T run() throws Exception;
    }
}
