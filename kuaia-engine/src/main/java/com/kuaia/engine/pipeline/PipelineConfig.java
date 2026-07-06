package com.kuaia.engine.pipeline;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PipelineConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String name;
    private final SourceConfig source;
    private final List<TransformConfig> transforms;
    private final SinkConfig sink;
    private final CheckpointConfig checkpoint;
    private final ErrorPolicyConfig errorPolicy;

    public PipelineConfig(String name, SourceConfig source, SinkConfig sink, CheckpointConfig checkpoint) {
        this(name, source, Collections.emptyList(), sink, checkpoint);
    }

    public PipelineConfig(
            String name,
            SourceConfig source,
            List<TransformConfig> transforms,
            SinkConfig sink,
            CheckpointConfig checkpoint) {
        this(name, source, transforms, sink, checkpoint, new ErrorPolicyConfig("fail-fast"));
    }

    public PipelineConfig(
            String name,
            SourceConfig source,
            List<TransformConfig> transforms,
            SinkConfig sink,
            CheckpointConfig checkpoint,
            ErrorPolicyConfig errorPolicy) {
        this.name = name;
        this.source = source;
        this.transforms = Collections.unmodifiableList(new ArrayList<>(transforms));
        this.sink = sink;
        this.checkpoint = checkpoint;
        this.errorPolicy = errorPolicy;
    }

    public String getName() {
        return name;
    }

    public SourceConfig getSource() {
        return source;
    }

    public List<TransformConfig> getTransforms() {
        return transforms;
    }

    public SinkConfig getSink() {
        return sink;
    }

    public CheckpointConfig getCheckpoint() {
        return checkpoint;
    }

    public ErrorPolicyConfig getErrorPolicy() {
        return errorPolicy;
    }

    public static class SourceConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String type;
        private final String path;
        private final String format;
        private final String documentType;
        private final String url;
        private final String userEnv;
        private final String passwordEnv;
        private final String query;
        private final int maxRowsPerSplit;
        private final int fetchSize;
        private final String endpoint;
        private final String region;
        private final String bucket;
        private final String prefix;
        private final String accessKeyEnv;
        private final String secretKeyEnv;
        private final boolean pathStyleAccess;

        public SourceConfig(String type, String path, String format) {
            this(type, path, format, 0);
        }

        public SourceConfig(String type, String path, String format, int maxRowsPerSplit) {
            this(type, path, format, null, null, null, null, maxRowsPerSplit, 0);
        }

        public SourceConfig(String type, String path, String format, String documentType) {
            this(type, path, format, null, null, null, null, 0, 0,
                    null, null, null, null, null, null, false, documentType);
        }

        /**
         * File source over a remote {@code s3://} path: keeps the URI verbatim in {@code path}
         * (bucket/key are parsed from it by {@code S3FileSystem}, so bucket/prefix stay {@code null})
         * and carries the S3 client settings. {@code documentType} is {@code null} for csv/jsonl.
         */
        public SourceConfig(
                String type,
                String path,
                String format,
                String documentType,
                String endpoint,
                String region,
                String accessKeyEnv,
                String secretKeyEnv,
                boolean pathStyleAccess) {
            this(type, path, format, documentType, 0,
                    endpoint, region, accessKeyEnv, secretKeyEnv, pathStyleAccess);
        }

        /**
         * File source over a remote {@code s3://} path that also carries {@code maxRowsPerSplit} for
         * csv/jsonl multi-splitting (0 for the {@code document} format). See the 9-arg overload for
         * how the URI, S3 client settings, and {@code documentType} are handled.
         */
        public SourceConfig(
                String type,
                String path,
                String format,
                String documentType,
                int maxRowsPerSplit,
                String endpoint,
                String region,
                String accessKeyEnv,
                String secretKeyEnv,
                boolean pathStyleAccess) {
            this(type, path, format, null, null, null, null, maxRowsPerSplit, 0,
                    endpoint, region, null, null, accessKeyEnv, secretKeyEnv, pathStyleAccess, documentType);
        }

        public SourceConfig(
                String type,
                String path,
                String format,
                String url,
                String userEnv,
                String passwordEnv,
                String query) {
            this(type, path, format, url, userEnv, passwordEnv, query, 0, 0);
        }

        public SourceConfig(
                String type,
                String path,
                String format,
                String url,
                String userEnv,
                String passwordEnv,
                String query,
                int maxRowsPerSplit) {
            this(type, path, format, url, userEnv, passwordEnv, query, maxRowsPerSplit, 0);
        }

        public SourceConfig(
                String type,
                String path,
                String format,
                String url,
                String userEnv,
                String passwordEnv,
                String query,
                int maxRowsPerSplit,
                int fetchSize) {
            this(type, path, format, url, userEnv, passwordEnv, query, maxRowsPerSplit, fetchSize,
                    null, null, null, null, null, null, false, null);
        }

        public SourceConfig(
                String type,
                String endpoint,
                String region,
                String bucket,
                String prefix,
                String accessKeyEnv,
                String secretKeyEnv,
                boolean pathStyleAccess) {
            this(type, null, null, null, null, null, null, 0, 0,
                    endpoint, region, bucket, prefix, accessKeyEnv, secretKeyEnv, pathStyleAccess, null);
        }

        private SourceConfig(
                String type,
                String path,
                String format,
                String url,
                String userEnv,
                String passwordEnv,
                String query,
                int maxRowsPerSplit,
                int fetchSize,
                String endpoint,
                String region,
                String bucket,
                String prefix,
                String accessKeyEnv,
                String secretKeyEnv,
                boolean pathStyleAccess,
                String documentType) {
            if (maxRowsPerSplit < 0) {
                throw new IllegalArgumentException("maxRowsPerSplit must not be negative");
            }
            if (fetchSize < 0) {
                throw new IllegalArgumentException("fetchSize must not be negative");
            }
            this.type = type;
            this.path = path;
            this.format = format;
            this.documentType = documentType;
            this.url = url;
            this.userEnv = userEnv;
            this.passwordEnv = passwordEnv;
            this.query = query;
            this.maxRowsPerSplit = maxRowsPerSplit;
            this.fetchSize = fetchSize;
            this.endpoint = endpoint;
            this.region = region;
            this.bucket = bucket;
            this.prefix = prefix;
            this.accessKeyEnv = accessKeyEnv;
            this.secretKeyEnv = secretKeyEnv;
            this.pathStyleAccess = pathStyleAccess;
        }

        public String getType() {
            return type;
        }

        public String getPath() {
            return path;
        }

        public String getFormat() {
            return format;
        }

        public String getDocumentType() {
            return documentType;
        }

        public String getUrl() {
            return url;
        }

        public String getUserEnv() {
            return userEnv;
        }

        public String getPasswordEnv() {
            return passwordEnv;
        }

        public String getQuery() {
            return query;
        }

        public int getMaxRowsPerSplit() {
            return maxRowsPerSplit;
        }

        public int getFetchSize() {
            return fetchSize;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public String getRegion() {
            return region;
        }

        public String getBucket() {
            return bucket;
        }

        public String getPrefix() {
            return prefix;
        }

        public String getAccessKeyEnv() {
            return accessKeyEnv;
        }

        public String getSecretKeyEnv() {
            return secretKeyEnv;
        }

        public boolean isPathStyleAccess() {
            return pathStyleAccess;
        }
    }

    public static class TransformConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String type;
        private final List<String> fields;
        private final String from;
        private final String to;
        private final String input;
        private final String output;
        private final int dimensions;
        private final String provider;
        private final String baseUrl;
        private final String model;
        private final String apiKeyEnv;
        private final int timeoutMs;
        private final int batchSize;
        private final int chunkSize;
        private final int overlap;
        private final boolean dropInput;
        private final boolean includeOffsets;
        private final String op;
        private final int minLength;
        private final String value;

        public TransformConfig(String type, List<String> fields, String from, String to) {
            this(type, fields, from, to, null, null, 4);
        }

        public TransformConfig(
                String type,
                List<String> fields,
                String from,
                String to,
                String input,
                String output,
                int dimensions) {
            this(type, fields, from, to, input, output, dimensions, null, null, null, null);
        }

        public TransformConfig(
                String type,
                List<String> fields,
                String from,
                String to,
                String input,
                String output,
                int dimensions,
                String provider,
                String baseUrl,
                String model,
                String apiKeyEnv) {
            this(type, fields, from, to, input, output, dimensions, provider, baseUrl, model, apiKeyEnv, 30000);
        }

        public TransformConfig(
                String type,
                List<String> fields,
                String from,
                String to,
                String input,
                String output,
                int dimensions,
                String provider,
                String baseUrl,
                String model,
                String apiKeyEnv,
                int timeoutMs) {
            this(type, fields, from, to, input, output, dimensions, provider, baseUrl, model, apiKeyEnv, timeoutMs, 32);
        }

        public TransformConfig(
                String type,
                List<String> fields,
                String from,
                String to,
                String input,
                String output,
                int dimensions,
                String provider,
                String baseUrl,
                String model,
                String apiKeyEnv,
                int timeoutMs,
                int batchSize) {
            this(type, fields, from, to, input, output, dimensions, provider, baseUrl, model, apiKeyEnv,
                    timeoutMs, batchSize, 0, 0);
        }

        public TransformConfig(
                String type,
                List<String> fields,
                String from,
                String to,
                String input,
                String output,
                int dimensions,
                String provider,
                String baseUrl,
                String model,
                String apiKeyEnv,
                int timeoutMs,
                int batchSize,
                int chunkSize,
                int overlap) {
            this(type, fields, from, to, input, output, dimensions, provider, baseUrl, model, apiKeyEnv,
                    timeoutMs, batchSize, chunkSize, overlap, false, false);
        }

        public TransformConfig(
                String type,
                List<String> fields,
                String from,
                String to,
                String input,
                String output,
                int dimensions,
                String provider,
                String baseUrl,
                String model,
                String apiKeyEnv,
                int timeoutMs,
                int batchSize,
                int chunkSize,
                int overlap,
                boolean dropInput,
                boolean includeOffsets) {
            this(type, fields, from, to, input, output, dimensions, provider, baseUrl, model, apiKeyEnv,
                    timeoutMs, batchSize, chunkSize, overlap, dropInput, includeOffsets, null);
        }

        public TransformConfig(
                String type,
                List<String> fields,
                String from,
                String to,
                String input,
                String output,
                int dimensions,
                String provider,
                String baseUrl,
                String model,
                String apiKeyEnv,
                int timeoutMs,
                int batchSize,
                int chunkSize,
                int overlap,
                boolean dropInput,
                boolean includeOffsets,
                String op) {
            this(type, fields, from, to, input, output, dimensions, provider, baseUrl, model, apiKeyEnv,
                    timeoutMs, batchSize, chunkSize, overlap, dropInput, includeOffsets, op, 0);
        }

        public TransformConfig(
                String type,
                List<String> fields,
                String from,
                String to,
                String input,
                String output,
                int dimensions,
                String provider,
                String baseUrl,
                String model,
                String apiKeyEnv,
                int timeoutMs,
                int batchSize,
                int chunkSize,
                int overlap,
                boolean dropInput,
                boolean includeOffsets,
                String op,
                int minLength) {
            this(type, fields, from, to, input, output, dimensions, provider, baseUrl, model, apiKeyEnv,
                    timeoutMs, batchSize, chunkSize, overlap, dropInput, includeOffsets, op, minLength, null);
        }

        public TransformConfig(
                String type,
                List<String> fields,
                String from,
                String to,
                String input,
                String output,
                int dimensions,
                String provider,
                String baseUrl,
                String model,
                String apiKeyEnv,
                int timeoutMs,
                int batchSize,
                int chunkSize,
                int overlap,
                boolean dropInput,
                boolean includeOffsets,
                String op,
                int minLength,
                String value) {
            this.type = type;
            this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
            this.from = from;
            this.to = to;
            this.input = input;
            this.output = output;
            this.dimensions = dimensions;
            this.provider = provider;
            this.baseUrl = baseUrl;
            this.model = model;
            this.apiKeyEnv = apiKeyEnv;
            this.timeoutMs = timeoutMs;
            this.batchSize = batchSize;
            this.chunkSize = chunkSize;
            this.overlap = overlap;
            this.dropInput = dropInput;
            this.includeOffsets = includeOffsets;
            this.op = op;
            this.minLength = minLength;
            this.value = value;
        }

        public String getType() {
            return type;
        }

        public List<String> getFields() {
            return fields;
        }

        public String getFrom() {
            return from;
        }

        public String getTo() {
            return to;
        }

        public String getInput() {
            return input;
        }

        public String getOutput() {
            return output;
        }

        public int getDimensions() {
            return dimensions;
        }

        public String getProvider() {
            return provider;
        }

        public String getBaseUrl() {
            return baseUrl;
        }

        public String getModel() {
            return model;
        }

        public String getApiKeyEnv() {
            return apiKeyEnv;
        }

        public int getTimeoutMs() {
            return timeoutMs;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public int getOverlap() {
            return overlap;
        }

        public boolean isDropInput() {
            return dropInput;
        }

        public boolean isIncludeOffsets() {
            return includeOffsets;
        }

        public String getOp() {
            return op;
        }

        public int getMinLength() {
            return minLength;
        }

        public String getValue() {
            return value;
        }
    }

    public static class SinkConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String type;
        private final String path;
        private final String format;
        private final String mode;
        private final String url;
        private final String collection;
        private final String apiKeyEnv;
        private final String idField;
        private final String vectorField;
        private final boolean wait;
        private final int timeoutMs;
        private final String chunkIndexField;
        private final long chunkIdMultiplier;
        private final List<String> payloadFields;
        private final String table;
        private final String userEnv;
        private final String passwordEnv;

        public SinkConfig(String type) {
            this(type, null, null, null);
        }

        public SinkConfig(String type, String path, String format, String mode) {
            this(type, path, format, mode, null, null, null, null, null, true, 0, null, 0L,
                    Collections.emptyList(), null, null, null);
        }

        public SinkConfig(
                String type,
                String path,
                String format,
                String mode,
                String url,
                String collection,
                String apiKeyEnv,
                String idField,
                String vectorField,
                boolean wait) {
            this(type, path, format, mode, url, collection, apiKeyEnv, idField, vectorField, wait, 0);
        }

        public SinkConfig(
                String type,
                String path,
                String format,
                String mode,
                String url,
                String collection,
                String apiKeyEnv,
                String idField,
                String vectorField,
                boolean wait,
                int timeoutMs) {
            this(type, path, format, mode, url, collection, apiKeyEnv, idField, vectorField, wait, timeoutMs, null, 0L,
                    Collections.emptyList());
        }

        public SinkConfig(
                String type,
                String path,
                String format,
                String mode,
                String url,
                String collection,
                String apiKeyEnv,
                String idField,
                String vectorField,
                boolean wait,
                int timeoutMs,
                String chunkIndexField,
                long chunkIdMultiplier) {
            this(type, path, format, mode, url, collection, apiKeyEnv, idField, vectorField, wait, timeoutMs,
                    chunkIndexField, chunkIdMultiplier, Collections.emptyList());
        }

        public SinkConfig(
                String type,
                String path,
                String format,
                String mode,
                String url,
                String collection,
                String apiKeyEnv,
                String idField,
                String vectorField,
                boolean wait,
                int timeoutMs,
                String chunkIndexField,
                long chunkIdMultiplier,
                List<String> payloadFields) {
            this(type, path, format, mode, url, collection, apiKeyEnv, idField, vectorField, wait, timeoutMs,
                    chunkIndexField, chunkIdMultiplier, payloadFields, null, null, null);
        }

        public SinkConfig(
                String type,
                String path,
                String format,
                String mode,
                String url,
                String collection,
                String apiKeyEnv,
                String idField,
                String vectorField,
                boolean wait,
                int timeoutMs,
                String chunkIndexField,
                long chunkIdMultiplier,
                List<String> payloadFields,
                String table,
                String userEnv,
                String passwordEnv) {
            if (timeoutMs < 0) {
                throw new IllegalArgumentException("timeoutMs must not be negative");
            }
            if (chunkIdMultiplier < 0) {
                throw new IllegalArgumentException("chunkIdMultiplier must not be negative");
            }
            this.type = type;
            this.path = path;
            this.format = format;
            this.mode = mode;
            this.url = url;
            this.collection = collection;
            this.apiKeyEnv = apiKeyEnv;
            this.idField = idField;
            this.vectorField = vectorField;
            this.wait = wait;
            this.timeoutMs = timeoutMs;
            this.chunkIndexField = chunkIndexField;
            this.chunkIdMultiplier = chunkIdMultiplier;
            this.payloadFields = Collections.unmodifiableList(new ArrayList<>(
                    payloadFields == null ? Collections.emptyList() : payloadFields));
            this.table = table;
            this.userEnv = userEnv;
            this.passwordEnv = passwordEnv;
        }

        public String getType() {
            return type;
        }

        public String getPath() {
            return path;
        }

        public String getFormat() {
            return format;
        }

        public String getMode() {
            return mode;
        }

        public String getUrl() {
            return url;
        }

        public String getCollection() {
            return collection;
        }

        public String getApiKeyEnv() {
            return apiKeyEnv;
        }

        public String getIdField() {
            return idField;
        }

        public String getVectorField() {
            return vectorField;
        }

        public boolean isWait() {
            return wait;
        }

        public int getTimeoutMs() {
            return timeoutMs;
        }

        public String getChunkIndexField() {
            return chunkIndexField;
        }

        public long getChunkIdMultiplier() {
            return chunkIdMultiplier;
        }

        public List<String> getPayloadFields() {
            return payloadFields;
        }

        public String getTable() {
            return table;
        }

        public String getUserEnv() {
            return userEnv;
        }

        public String getPasswordEnv() {
            return passwordEnv;
        }
    }

    public static class CheckpointConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String stateDir;

        public CheckpointConfig(String stateDir) {
            this.stateDir = stateDir;
        }

        public String getStateDir() {
            return stateDir;
        }
    }

    public static class ErrorPolicyConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String mode;

        public ErrorPolicyConfig(String mode) {
            this.mode = mode;
        }

        public String getMode() {
            return mode;
        }

        public boolean shouldSkipBadRecords() {
            return "skip-bad-records".equals(mode);
        }
    }
}
