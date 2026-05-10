package com.kuaia.engine.pipeline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineConfigLoader {
    private static final String DEFAULT_OPENAI_COMPATIBLE_BASE_URL = "https://api.openai.com/v1";
    private static final int DEFAULT_OPENAI_COMPATIBLE_TIMEOUT_MS = 30_000;
    private static final int DEFAULT_QDRANT_TIMEOUT_MS = 30_000;
    private static final int DEFAULT_EMBEDDING_BATCH_SIZE = 32;
    private static final String RESTRICT_LOCAL_PATHS_ENV = "KUAIA_RESTRICT_LOCAL_PATHS";

    private final boolean restrictLocalPaths;

    public PipelineConfigLoader() {
        this(isRestrictedLocalPathsEnabled());
    }

    PipelineConfigLoader(boolean restrictLocalPaths) {
        this.restrictLocalPaths = restrictLocalPaths;
    }

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

        String stateDir = checkpoint == null ? null : resolveCheckpointPath(path, checkpoint.get("stateDir"));
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
                        null,
                        DEFAULT_OPENAI_COMPATIBLE_TIMEOUT_MS,
                        parseBatchSize(transform.get("batchSize"))));
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
            if (hasText(source.get("maxRowsPerSplit"))) {
                throw new PipelineConfigException("source.maxRowsPerSplit is only supported for source.type: file");
            }
            return new PipelineConfig.SourceConfig(
                    "postgres",
                    null,
                    null,
                    require(source, "source.url"),
                    require(source, "source.userEnv"),
                    require(source, "source.passwordEnv"),
                    require(source, "source.query"),
                    0,
                    parseSourceFetchSize(source.get("fetchSize")));
        }

        if (hasText(source.get("fetchSize"))) {
            throw new PipelineConfigException("source.fetchSize is only supported for source.type: postgres");
        }
        String sourcePath = resolveLocalPath(configPath, require(source, "source.path"), "source.path");
        String sourceFormat = require(source, "source.format");
        requireSupported("source.format", sourceFormat, "csv");
        return new PipelineConfig.SourceConfig(
                sourceType,
                sourcePath,
                sourceFormat,
                parseSourceMaxRowsPerSplit(source.get("maxRowsPerSplit")));
    }

    private PipelineConfig.TransformConfig loadEmbeddingTransform(Map<String, String> transform, String fieldPrefix)
            throws PipelineConfigException {
        String provider = require(transform, fieldPrefix + ".provider");
        requireSupported(fieldPrefix + ".provider", provider, "mock", "openai-compatible");

        String input = require(transform, fieldPrefix + ".input");
        String output = require(transform, fieldPrefix + ".output");
        int defaultDimensions = "mock".equals(provider) ? 4 : 0;
        int dimensions = parseDimensions(transform.get("dimensions"), defaultDimensions);
        int batchSize = parseBatchSize(transform.get("batchSize"));

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
                    null,
                    DEFAULT_OPENAI_COMPATIBLE_TIMEOUT_MS,
                    batchSize);
        }

        String baseUrl = transform.get("baseUrl");
        if (baseUrl == null || baseUrl.trim().isEmpty()) {
            baseUrl = DEFAULT_OPENAI_COMPATIBLE_BASE_URL;
        }
        int timeoutMs = parseTimeoutMs(transform.get("timeoutMs"));
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
                require(transform, fieldPrefix + ".apiKeyEnv"),
                timeoutMs,
                batchSize);
    }

    private PipelineConfig.SinkConfig loadSink(Path configPath, String sinkType, Map<String, String> sink)
            throws PipelineConfigException {
        if ("qdrant".equals(sinkType)) {
            return loadQdrantSink(sink);
        }
        if (hasText(sink.get("timeoutMs"))) {
            throw new PipelineConfigException("sink.timeoutMs is only supported for sink.type: qdrant");
        }
        if (!"file".equals(sinkType)) {
            return new PipelineConfig.SinkConfig(sinkType);
        }

        String sinkPath = resolveLocalPath(configPath, require(sink, "sink.path"), "sink.path");
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
                waitForCommit,
                parseSinkTimeoutMs(sink.get("timeoutMs"), DEFAULT_QDRANT_TIMEOUT_MS));
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

    private int parseTimeoutMs(String value) throws PipelineConfigException {
        if (value == null || value.trim().isEmpty()) {
            return DEFAULT_OPENAI_COMPATIBLE_TIMEOUT_MS;
        }
        try {
            int timeoutMs = Integer.parseInt(value.trim());
            if (timeoutMs <= 0) {
                throw new PipelineConfigException("Invalid transform.timeoutMs: " + value);
            }
            return timeoutMs;
        } catch (NumberFormatException e) {
            throw new PipelineConfigException("Invalid transform.timeoutMs: " + value, e);
        }
    }

    private int parseBatchSize(String value) throws PipelineConfigException {
        if (value == null || value.trim().isEmpty()) {
            return DEFAULT_EMBEDDING_BATCH_SIZE;
        }
        try {
            int batchSize = Integer.parseInt(value.trim());
            if (batchSize <= 0) {
                throw new PipelineConfigException("Invalid transform.batchSize: " + value);
            }
            return batchSize;
        } catch (NumberFormatException e) {
            throw new PipelineConfigException("Invalid transform.batchSize: " + value, e);
        }
    }

    private int parseSinkTimeoutMs(String value, int defaultTimeoutMs) throws PipelineConfigException {
        if (value == null || value.trim().isEmpty()) {
            return defaultTimeoutMs;
        }
        try {
            int timeoutMs = Integer.parseInt(value.trim());
            if (timeoutMs <= 0) {
                throw new PipelineConfigException("Invalid sink.timeoutMs: " + value);
            }
            return timeoutMs;
        } catch (NumberFormatException e) {
            throw new PipelineConfigException("Invalid sink.timeoutMs: " + value, e);
        }
    }

    private int parseSourceMaxRowsPerSplit(String value) throws PipelineConfigException {
        if (value == null || value.trim().isEmpty()) {
            return 0;
        }
        try {
            int maxRowsPerSplit = Integer.parseInt(value.trim());
            if (maxRowsPerSplit <= 0) {
                throw new PipelineConfigException("Invalid source.maxRowsPerSplit: " + value);
            }
            return maxRowsPerSplit;
        } catch (NumberFormatException e) {
            throw new PipelineConfigException("Invalid source.maxRowsPerSplit: " + value, e);
        }
    }

    private int parseSourceFetchSize(String value) throws PipelineConfigException {
        if (value == null || value.trim().isEmpty()) {
            return 0;
        }
        try {
            int fetchSize = Integer.parseInt(value.trim());
            if (fetchSize <= 0) {
                throw new PipelineConfigException("Invalid source.fetchSize: " + value);
            }
            return fetchSize;
        } catch (NumberFormatException e) {
            throw new PipelineConfigException("Invalid source.fetchSize: " + value, e);
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

    private String resolveLocalPath(Path configPath, String localPath, String field) throws PipelineConfigException {
        Path path = Paths.get(localPath);
        String resolved;
        if (path.isAbsolute()) {
            resolved = path.toString();
        } else {
            Path parent = configPath.toAbsolutePath().getParent();
            if (parent == null) {
                resolved = path.normalize().toString();
            } else {
                resolved = parent.resolve(path).normalize().toString();
            }
        }
        Path resolvedPath = Paths.get(resolved);
        requireAllowedLocalPath(configPath, resolvedPath, field);
        if (restrictLocalPaths) {
            return resolvedPath.toAbsolutePath().normalize().toString();
        }
        return resolved;
    }

    private String resolveCheckpointPath(Path configPath, String stateDir) throws PipelineConfigException {
        if (stateDir == null || stateDir.trim().isEmpty() || !restrictLocalPaths) {
            return stateDir;
        }
        Path statePath = Paths.get(stateDir);
        Path resolved;
        if (statePath.isAbsolute()) {
            resolved = statePath.normalize();
        } else if (statePath.getNameCount() > 0 && ".kuaia".equals(statePath.getName(0).toString())) {
            resolved = findRepoRoot(configPath.toAbsolutePath().getParent()).resolve(statePath).normalize();
        } else {
            Path parent = configPath.toAbsolutePath().getParent();
            resolved = parent == null ? statePath.toAbsolutePath().normalize() : parent.resolve(statePath).normalize();
        }
        requireAllowedLocalPath(configPath, resolved, "checkpoint.stateDir");
        return resolved.toString();
    }

    private void requireAllowedLocalPath(Path configPath, Path path, String field) throws PipelineConfigException {
        if (!restrictLocalPaths) {
            return;
        }
        Path normalized = path.toAbsolutePath().normalize();
        Path configDir = configPath.toAbsolutePath().getParent();
        if (configDir == null) {
            configDir = Paths.get("").toAbsolutePath();
        }
        configDir = configDir.normalize();
        Path repoKuaiaDir = findRepoRoot(configDir).resolve(".kuaia").normalize();
        if (normalized.startsWith(configDir) || normalized.startsWith(repoKuaiaDir)) {
            return;
        }
        throw new PipelineConfigException("Local path escapes allowed directories: " + field);
    }

    private Path findRepoRoot(Path start) {
        Path current = start == null ? Paths.get("").toAbsolutePath().normalize() : start.toAbsolutePath().normalize();
        while (current != null) {
            if (Files.exists(current.resolve("pom.xml")) && Files.isDirectory(current.resolve("kuaia-engine"))) {
                return current;
            }
            current = current.getParent();
        }
        return Paths.get("").toAbsolutePath().normalize();
    }

    private static boolean isRestrictedLocalPathsEnabled() {
        String value = System.getenv(RESTRICT_LOCAL_PATHS_ENV);
        return value != null && "true".equalsIgnoreCase(value.trim());
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
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
