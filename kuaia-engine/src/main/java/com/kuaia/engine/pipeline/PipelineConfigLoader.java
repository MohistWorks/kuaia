package com.kuaia.engine.pipeline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineConfigLoader {
    public PipelineConfig load(Path path) throws PipelineConfigException {
        if (!Files.exists(path)) {
            throw new PipelineConfigException("Pipeline config not found: " + path);
        }

        Map<String, String> topLevel = new HashMap<>();
        Map<String, Map<String, String>> sections = new HashMap<>();
        try {
            parse(Files.readAllLines(path, StandardCharsets.UTF_8), topLevel, sections);
        } catch (IOException e) {
            throw new PipelineConfigException("Failed to read pipeline config: " + path, e);
        }

        String name = require(topLevel, "name");
        Map<String, String> source = section(sections, "source");
        Map<String, String> sink = section(sections, "sink");
        Map<String, String> checkpoint = sections.get("checkpoint");

        String sourceType = require(source, "source.type");
        String sourcePath = resolveSourcePath(path, require(source, "source.path"));
        String sourceFormat = require(source, "source.format");
        String sinkType = require(sink, "sink.type");

        requireSupported("source.type", sourceType, "file");
        requireSupported("source.format", sourceFormat, "csv");
        requireSupported("sink.type", sinkType, "console");

        String stateDir = checkpoint == null ? null : checkpoint.get("stateDir");
        return new PipelineConfig(
                name,
                new PipelineConfig.SourceConfig(sourceType, sourcePath, sourceFormat),
                new PipelineConfig.SinkConfig(sinkType),
                new PipelineConfig.CheckpointConfig(stateDir));
    }

    private void parse(
            List<String> lines,
            Map<String, String> topLevel,
            Map<String, Map<String, String>> sections) {
        String currentSection = null;
        for (String rawLine : lines) {
            if (rawLine.trim().isEmpty() || rawLine.trim().startsWith("#")) {
                continue;
            }
            if (!rawLine.startsWith(" ")) {
                String[] entry = splitKeyValue(rawLine.trim());
                if (entry[1].isEmpty()) {
                    currentSection = entry[0];
                    sections.put(currentSection, new HashMap<>());
                } else {
                    currentSection = null;
                    topLevel.put(entry[0], stripQuotes(entry[1]));
                }
                continue;
            }

            if (currentSection != null && rawLine.startsWith("  ")) {
                String[] entry = splitKeyValue(rawLine.trim());
                sections.get(currentSection).put(entry[0], stripQuotes(entry[1]));
            }
        }
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

    private void requireSupported(String field, String value, String supported) throws PipelineConfigException {
        if (!supported.equals(value)) {
            throw new PipelineConfigException("Unsupported " + field + ": " + value);
        }
    }

    private String resolveSourcePath(Path configPath, String sourcePath) {
        Path path = java.nio.file.Paths.get(sourcePath);
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
