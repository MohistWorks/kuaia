package com.kuaia.engine.pipeline;

public class PipelineConfig {
    private final String name;
    private final SourceConfig source;
    private final SinkConfig sink;
    private final CheckpointConfig checkpoint;

    public PipelineConfig(String name, SourceConfig source, SinkConfig sink, CheckpointConfig checkpoint) {
        this.name = name;
        this.source = source;
        this.sink = sink;
        this.checkpoint = checkpoint;
    }

    public String getName() {
        return name;
    }

    public SourceConfig getSource() {
        return source;
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
