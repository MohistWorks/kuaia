package com.kuaia.engine.worker.connector;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Local-filesystem backing for {@link KuaiaFileSystem}. Depends on {@code java.nio} only so it can
 * live in the dependency-light connectors module. Accepts both bare filesystem paths (what the
 * config loader already produces via {@code resolveLocalPath}) and {@code file://} URIs.
 */
public class LocalFileSystem implements KuaiaFileSystem {

    @Override
    public boolean exists(String uri) {
        return Files.exists(toPath(uri));
    }

    @Override
    public boolean isDirectory(String uri) {
        return Files.isDirectory(toPath(uri));
    }

    @Override
    public List<String> list(String uri) {
        Path root = toPath(uri);
        try (Stream<Path> stream = Files.walk(root)) {
            return stream.filter(Files::isRegularFile)
                    .map(Path::toString)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public byte[] readAllBytes(String uri) throws IOException {
        return Files.readAllBytes(toPath(uri));
    }

    private static Path toPath(String uri) {
        if (uri.startsWith("file://")) {
            return Paths.get(URI.create(uri));
        }
        return Paths.get(uri);
    }
}
