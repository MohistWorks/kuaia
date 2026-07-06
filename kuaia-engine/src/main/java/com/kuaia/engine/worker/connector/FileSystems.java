package com.kuaia.engine.worker.connector;

import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

/**
 * Selects the {@link KuaiaFileSystem} backend for a {@code file} source from its {@code path} scheme:
 * {@code s3://} -> {@link S3FileSystem}, {@code file://} or a bare path -> {@link LocalFileSystem},
 * {@code hdfs://} -> reserved (not yet supported), anything else -> unsupported.
 *
 * <p>The config loader rejects unsupported schemes up front; this resolver is the defense-in-depth
 * path for tasks replayed by a coordinator that bypass the YAML loader (mirroring the migration
 * throws in {@link com.kuaia.engine.pipeline.ConnectorFactory}).
 */
public final class FileSystems {
    private FileSystems() {}

    public static KuaiaFileSystem forSource(PipelineConfig.SourceConfig config) throws PipelineExecutionException {
        String scheme = UriSchemes.schemeOf(config.getPath());
        if ("s3".equals(scheme)) {
            return new S3FileSystem(config);
        }
        if (scheme == null || "file".equals(scheme)) {
            return new LocalFileSystem();
        }
        if ("hdfs".equals(scheme)) {
            throw new PipelineExecutionException("source.path storage scheme hdfs:// is not yet supported");
        }
        throw new PipelineExecutionException("source.path storage scheme " + scheme + ":// is not supported");
    }
}
