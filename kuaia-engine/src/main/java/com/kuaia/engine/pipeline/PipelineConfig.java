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

    public PipelineConfig(String name, SourceConfig source, SinkConfig sink, CheckpointConfig checkpoint) {
        this(name, source, Collections.emptyList(), sink, checkpoint);
    }

    public PipelineConfig(
            String name,
            SourceConfig source,
            List<TransformConfig> transforms,
            SinkConfig sink,
            CheckpointConfig checkpoint) {
        this.name = name;
        this.source = source;
        this.transforms = Collections.unmodifiableList(new ArrayList<>(transforms));
        this.sink = sink;
        this.checkpoint = checkpoint;
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
            this.type = type;
            this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
            this.from = from;
            this.to = to;
            this.input = input;
            this.output = output;
            this.dimensions = dimensions;
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
    }

    public static class SinkConfig {
        private final String type;

        public SinkConfig(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
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
}
