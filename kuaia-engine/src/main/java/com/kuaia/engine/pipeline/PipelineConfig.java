package com.kuaia.engine.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PipelineConfig {
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

    public static class SourceConfig {
        private final String type;
        private final String path;
        private final String format;

        public SourceConfig(String type, String path, String format) {
            this.type = type;
            this.path = path;
            this.format = format;
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
    }

    public static class TransformConfig {
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
    }

    public static class SinkConfig {
        private final String type;
        private final String path;
        private final String format;
        private final String mode;

        public SinkConfig(String type) {
            this(type, null, null, null);
        }

        public SinkConfig(String type, String path, String format, String mode) {
            this.type = type;
            this.path = path;
            this.format = format;
            this.mode = mode;
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
    }

    public static class CheckpointConfig {
        private final String stateDir;

        public CheckpointConfig(String stateDir) {
            this.stateDir = stateDir;
        }

        public String getStateDir() {
            return stateDir;
        }
    }

    public static class ErrorPolicyConfig {
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
