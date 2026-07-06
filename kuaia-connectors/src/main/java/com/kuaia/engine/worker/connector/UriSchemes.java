package com.kuaia.engine.worker.connector;

import java.util.Locale;

/**
 * Shared URI-scheme parsing for storage paths ({@code s3://…}, {@code file://…}, {@code hdfs://…},
 * or a bare filesystem path). Dependency-free (java.util only) so it can live in the connectors
 * module and be reached by both the engine (config loader, validator) and the connector file-system
 * resolver, keeping the {@code indexOf("://")} convention in exactly one place.
 */
public final class UriSchemes {
    private UriSchemes() {}

    /**
     * @return the lowercased URI scheme of {@code path} (e.g. {@code "s3"}), or {@code null} when the
     *     path carries no scheme (a bare filesystem path). Matches {@code path.indexOf("://")} with
     *     {@code idx <= 0} treated as "no scheme".
     */
    public static String schemeOf(String path) {
        if (path == null) {
            return null;
        }
        int idx = path.indexOf("://");
        if (idx <= 0) {
            return null;
        }
        return path.substring(0, idx).toLowerCase(Locale.ROOT);
    }

    /**
     * @return {@code true} when {@code path} has a remote (non-{@code file}) scheme; {@code false} for
     *     a bare path or a {@code file://} URI.
     */
    public static boolean isRemote(String path) {
        String scheme = schemeOf(path);
        return scheme != null && !"file".equals(scheme);
    }
}
