package com.kuaia.engine.worker.connector;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory {@link KuaiaFileSystem} test double keyed by full URI (e.g. {@code s3://bucket/docs/a.md}
 * -> bytes). It honors the same-string-space {@code list} contract: {@link #list(String)} returns the
 * seeded child URIs verbatim (each already prefixed by the argument), so a {@code DocumentSource} or
 * {@code FileSource} built over it exercises the exact relativization callers use against a real
 * remote backend.
 */
final class InMemoryFileSystem implements KuaiaFileSystem {
    private final Map<String, byte[]> objects = new LinkedHashMap<>();

    InMemoryFileSystem put(String uri, String content) {
        objects.put(uri, content.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    @Override
    public boolean exists(String uri) {
        if (uri.endsWith("/")) {
            return objects.keySet().stream().anyMatch(key -> key.startsWith(uri));
        }
        return objects.containsKey(uri);
    }

    @Override
    public boolean isDirectory(String uri) {
        return uri.endsWith("/");
    }

    @Override
    public List<String> list(String uri) {
        List<String> children = new ArrayList<>();
        for (String key : objects.keySet()) {
            if (key.startsWith(uri) && !key.equals(uri)) {
                children.add(key);
            }
        }
        return children;
    }

    @Override
    public byte[] readAllBytes(String uri) throws FileNotFoundException {
        byte[] bytes = objects.get(uri);
        if (bytes == null) {
            throw new FileNotFoundException(uri);
        }
        return bytes;
    }
}
