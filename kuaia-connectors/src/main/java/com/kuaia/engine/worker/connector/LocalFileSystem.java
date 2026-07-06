package com.kuaia.engine.worker.connector;

import java.io.IOException;
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
    public List<String> list(String uri) throws IOException {
        Path root = toPath(uri);
        // Return children in the same string space as the argument: strip any trailing separator
        // from the input root and re-join it with each file's forward-slashed relative sub-path. A
        // bare-path root therefore yields bare children (byte-identical to Path.toString()) while a
        // file:// root yields file:// children, matching the s3:// prefix -> s3:// children contract.
        String base = stripTrailingSeparators(uri);
        try (Stream<Path> stream = Files.walk(root)) {
            return stream.filter(Files::isRegularFile)
                    .map(child -> base + "/" + root.relativize(child).toString().replace('\\', '/'))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public byte[] readAllBytes(String uri) throws IOException {
        return Files.readAllBytes(toPath(uri));
    }

    private static String stripTrailingSeparators(String uri) {
        int end = uri.length();
        while (end > 0 && (uri.charAt(end - 1) == '/' || uri.charAt(end - 1) == '\\')) {
            end--;
        }
        return uri.substring(0, end);
    }

    private static Path toPath(String uri) {
        if (uri.startsWith("file://")) {
            return Paths.get(URI.create(uri));
        }
        return Paths.get(uri);
    }
}
