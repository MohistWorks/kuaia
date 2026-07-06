package com.kuaia.engine.worker.connector;

import java.util.List;

/**
 * Storage SPI behind the {@code file} source. The {@code uri} strings identify objects; the scheme
 * (or lack of one) selects the backend. Local paths and {@code file://} URIs both map to the local
 * filesystem via {@link LocalFileSystem}. All reads route through this interface instead of touching
 * {@link java.nio.file.Path} directly so alternate backends (S3, HDFS, ...) can be plugged in later.
 */
public interface KuaiaFileSystem extends AutoCloseable {
    /** @return {@code true} if an object exists at {@code uri}. */
    boolean exists(String uri) throws Exception;

    /** @return {@code true} if {@code uri} denotes a directory/prefix rather than a single object. */
    boolean isDirectory(String uri) throws Exception;

    /**
     * Returns the URIs of all regular files under {@code uri}, each prefixed by {@code uri} (same
     * scheme/string space as the argument), so callers may relativize by stripping the argument
     * prefix. Listing is recursive and ordering is left to the caller.
     */
    List<String> list(String uri) throws Exception;

    /** Read the whole object at {@code uri} into memory (matches the current eager-buffer semantics). */
    byte[] readAllBytes(String uri) throws Exception;

    @Override
    default void close() {}
}
