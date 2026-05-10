package com.kuaia.engine.pipeline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineConfigLoader {
    private static final String DEFAULT_OPENAI_COMPATIBLE_BASE_URL = "https://api.openai.com/v1";

    public PipelineConfig load(Path path) throws PipelineConfigException {
        if (!Files.exists(path)) {
            throw new PipelineConfigException("Pipeline config not found: " + path);
        }

        Map<String, String> topLevel = new HashMap<>();
        Map<String, Map<String, String>> sections = new HashMap<>();
        List<Map<String, String>> transforms = new ArrayList<>();
        try {
            parse(Files.readAllLines(path, StandardCharsets.UTF_8), topLevel, sections, transforms);
        } catch (IOException e) {
            throw new PipelineConfigException("Failed to read pipeline config: " + path, e);
        }

        String name = require(topLevel, "name");
        Map<String, String> source = section(sections, "source");
        Map<String, String> sink = section(sections, "sink");
        Map<String, String> checkpoint = sections.get("checkpoint");
        Map<String, String> errorPolicy = sections.get("errorPolicy");

        String sourceType = require(source, "source.type");
        String sinkType = require(sink, "sink.type");

        requireSupported("source.type", sourceType, "file", "postgres");
        requireSupported("sink.type", sinkType, "console", "mock-vector", "file", "qdrant");

        PipelineConfig.SourceConfig sourceConfig = loadSource(path, sourceType, source);
        PipelineConfig.SinkConfig sinkConfig = loadSink(path, sinkType, sink);
        PipelineConfig.ErrorPolicyConfig errorPolicyConfig = loadErrorPolicy(errorPolicy);

        String stateDir = checkpoint == null ? null : checkpoint.get("stateDir");
        return new PipelineConfig(
                name,
                sourceConfig,
                loadTransforms(transforms),
                sinkConfig,
                new PipelineConfig.CheckpointConfig(stateDir),
                errorPolicyConfig);
    }

    private void parse(
            List<String> lines,
            Map<String, String> topLevel,
            Map<String, Map<String, String>> sections,
            List<Map<String, String>> transforms) {
        String currentSection = null;
        Map<String, String> currentTransform = null;
        for (String rawLine : lines) {
            if (rawLine.trim().isEmpty() || rawLine.trim().startsWith("#")) {
                continue;
            }
            if (!rawLine.startsWith(" ")) {
                String[] entry = splitKeyValue(rawLine.trim());
                if (entry[1].isEmpty()) {
                    currentSection = entry[0];
                    currentTransform = null;
                    if (!"transforms".equals(currentSection)) {
                        sections.put(currentSection, new HashMap<>());
                    }
                } else {
                    currentSection = null;
                    currentTransform = null;
                    topLevel.put(entry[0], stripQuotes(entry[1]));
                }
                continue;
            }

            if ("transforms".equals(currentSection)) {
                if (rawLine.startsWith("  - ")) {
                    currentTransform = new HashMap<>();
                    transforms.add(currentTransform);
                    String[] entry = splitKeyValue(rawLine.substring(4).trim());
                    if (!entry[0].isEmpty()) {
                        currentTransform.put(entry[0], stripQuotes(entry[1]));
                    }
                } else if (currentTransform != null && rawLine.startsWith("    ")) {
                    String[] entry = splitKeyValue(rawLine.trim());
                    currentTransform.put(entry[0], stripQuotes(entry[1]));
                }
                continue;
            }

            if (currentSection != null && rawLine.startsWith("  ")) {
                String[] entry = splitKeyValue(rawLine.trim());
                sections.get(currentSection).put(entry[0], stripQuotes(entry[1]));
            }
        }
    }

    private List<PipelineConfig.TransformConfig> loadTransforms(List<Map<String, String>> transforms)
            throws PipelineConfigException {
        List<PipelineConfig.TransformConfig> configs = new ArrayList<>();
        for (int i = 0; i < transforms.size(); i++) {
            Map<String, String> transform = transforms.get(i);
            String fieldPrefix = "transforms[" + i + "]";
            String type = require(transform, fieldPrefix + ".type");
            if ("select".equals(type)) {
                List<String> fields = parseInlineList(require(transform, fieldPrefix + ".fields"), fieldPrefix + ".fields");
                configs.add(new PipelineConfig.TransformConfig(type, fields, null, null));
            } else if ("rename".equals(type)) {
                configs.add(new PipelineConfig.TransformConfig(
                        type,
                        new ArrayList<>(),
                        require(transform, fieldPrefix + ".from"),
                        require(transform, fieldPrefix + ".to")));
            } else if ("mock-embedding".equals(type)) {
                configs.add(new PipelineConfig.TransformConfig(
                        type,
                        new ArrayList<>(),
                        null,
                        null,
                        require(transform, fieldPrefix + ".input"),
                        require(transform, fieldPrefix + ".output"),
                        parseDimensions(transform.get("dimensions"), 4),
                        "mock",
                        null,
                        null,
                        null));
            } else if ("embedding".equals(type)) {
                configs.add(loadEmbeddingTransform(transform, fieldPrefix));
            } else {
                throw new PipelineConfigException("Unsupported transform.type: " + type);
            }
        }
        return configs;
    }

    private PipelineConfig.SourceConfig loadSource(Path configPath, String sourceType, Map<String, String> source)
            throws PipelineConfigException {
        if ("postgres".equals(sourceType)) {
            return new PipelineConfig.SourceConfig(
                    "postgres",
                    null,
                    null,
                    require(source, "source.url"),
                    require(source, "source.userEnv"),
                    require(source, "source.passwordEnv"),
                    require(source, "source.query"));
        }

        String sourcePath = resolveLocalPath(configPath, require(source, "source.path"));
        String sourceFormat = require(source, "source.format");
        requireSupported("source.format", sourceFormat, "csv");
        return new PipelineConfig.SourceConfig(sourceType, sourcePath, sourceFormat);
    }

    private PipelineConfig.TransformConfig loadEmbeddingTransform(Map<String, String> transform, String fieldPrefix)
            throws PipelineConfigException {
        String provider = require(transform, fieldPrefix + ".provider");
        requireSupported(fieldPrefix + ".provider", provider, "mock", "openai-compatible");

        String input = require(transform, fieldPrefix + ".input");
        String output = require(transform, fieldPrefix + ".output");
        int defaultDimensions = "mock".equals(provider) ? 4 : 0;
        int dimensions = parseDimensions(transform.get("dimensions"), defaultDimensions);

        if ("mock".equals(provider)) {
            return new PipelineConfig.TransformConfig(
                    "embedding",
                    new ArrayList<>(),
                    null,
                    null,
                    input,
                    output,
                    dimensions,
                    provider,
                    null,
                    null,
                    null);
        }

        String baseUrl = transform.get("baseUrl");
        if (baseUrl == null || baseUrl.trim().isEmpty()) {
            baseUrl = DEFAULT_OPENAI_COMPATIBLE_BASE_URL;
        }
        return new PipelineConfig.TransformConfig(
                "embedding",
                new ArrayList<>(),
                null,
                null,
                input,
                output,
                dimensions,
                provider,
                baseUrl,
                require(transform, fieldPrefix + ".model"),
                require(transform, fieldPrefix + ".apiKeyEnv"));
    }

    private PipelineConfig.SinkConfig loadSink(Path configPath, String sinkType, Map<String, String> sink)
            throws PipelineConfigException {
        if ("qdrant".equals(sinkType)) {
            return loadQdrantSink(sink);
        }
        if (!"file".equals(sinkType)) {
            return new PipelineConfig.SinkConfig(sinkType);
        }

        String sinkPath = resolveLocalPath(configPath, require(sink, "sink.path"));
        String sinkFormat = require(sink, "sink.format");
        requireSupported("sink.format", sinkFormat, "csv");

        String mode = sink.get("mode");
        if (mode == null || mode.trim().isEmpty()) {
            mode = "overwrite";
        }
        requireSupported("sink.mode", mode, "overwrite", "append");

        return new PipelineConfig.SinkConfig(sinkType, sinkPath, sinkFormat, mode);
    }

    private PipelineConfig.SinkConfig loadQdrantSink(Map<String, String> sink) throws PipelineConfigException {
        String wait = sink.get("wait");
        boolean waitForCommit = true;
        if (wait != null && !wait.trim().isEmpty()) {
            requireSupported("sink.wait", wait, "true", "false");
            waitForCommit = Boolean.parseBoolean(wait);
        }
        return new PipelineConfig.SinkConfig(
                "qdrant",
                null,
                null,
                null,
                require(sink, "sink.url"),
                require(sink, "sink.collection"),
                sink.get("apiKeyEnv"),
                require(sink, "sink.idField"),
                require(sink, "sink.vectorField"),
                waitForCommit);
    }

    private PipelineConfig.ErrorPolicyConfig loadErrorPolicy(Map<String, String> errorPolicy)
            throws PipelineConfigException {
        String mode = "fail-fast";
        if (errorPolicy != null) {
            String configuredMode = errorPolicy.get("mode");
            if (configuredMode != null && !configuredMode.trim().isEmpty()) {
                mode = configuredMode;
            }
        }
        requireSupported("errorPolicy.mode", mode, "fail-fast", "skip-bad-records");
        return new PipelineConfig.ErrorPolicyConfig(mode);
    }

    private List<String> parseInlineList(String value, String field) throws PipelineConfigException {
        String trimmed = value.trim();
        if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
            throw new PipelineConfigException("Missing required field: " + field);
        }
        String body = trimmed.substring(1, trimmed.length() - 1).trim();
        if (body.isEmpty()) {
            throw new PipelineConfigException("Missing required field: " + field);
        }
        List<String> values = new ArrayList<>();
        for (String rawItem : body.split(",")) {
            String item = stripQuotes(rawItem.trim());
            if (item.isEmpty()) {
                throw new PipelineConfigException("Missing required field: " + field);
            }
            values.add(item);
        }
        return values;
    }

    private String[] splitKeyValue(String line) {
        int separator = line.indexOf(':');
        if (separator < 0) {
            return new String[]{line, ""};
        }
        String key = line.substring(0, separator).trim();
        String value = line.substring(separator + 1).trim();
        return new String[]{key, value};
    }

    private Map<String, String> section(Map<String, Map<String, String>> sections, String name)
            throws PipelineConfigException {
        Map<String, String> section = sections.get(name);
        if (section == null) {
            throw new PipelineConfigException("Missing required field: " + name);
        }
        return section;
    }

    private String require(Map<String, String> values, String field) throws PipelineConfigException {
        String key = field.contains(".") ? field.substring(field.indexOf('.') + 1) : field;
        String value = values.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new PipelineConfigException("Missing required field: " + field);
        }
        return value;
    }

    private int parseDimensions(String value) throws PipelineConfigException {
        return parseDimensions(value, 4);
    }

    private int parseDimensions(String value, int defaultDimensions) throws PipelineConfigException {
        if (value == null || value.trim().isEmpty()) {
            return defaultDimensions;
        }
        try {
            int dimensions = Integer.parseInt(value.trim());
            if (dimensions <= 0) {
                throw new PipelineConfigException("Invalid transform.dimensions: " + value);
            }
            return dimensions;
        } catch (NumberFormatException e) {
            throw new PipelineConfigException("Invalid transform.dimensions: " + value, e);
        }
    }

    private void requireSupported(String field, String value, String... supported) throws PipelineConfigException {
        for (String candidate : supported) {
            if (candidate.equals(value)) {
                return;
            }
        }
        if (field.endsWith(".type")) {
            throw new PipelineConfigException("Unsupported " + field + ": " + value);
        }
        if (!supported[0].equals(value)) {
            throw new PipelineConfigException("Unsupported " + field + ": " + value);
        }
    }

    private String resolveLocalPath(Path configPath, String localPath) {
        Path path = java.nio.file.Paths.get(localPath);
        if (path.isAbsolute()) {
            return path.toString();
        }
        Path parent = configPath.toAbsolutePath().getParent();
        if (parent == null) {
            return path.normalize().toString();
        }
        return parent.resolve(path).normalize().toString();
    }

    private String stripQuotes(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }
}
