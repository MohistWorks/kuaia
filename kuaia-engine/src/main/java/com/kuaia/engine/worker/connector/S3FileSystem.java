package com.kuaia.engine.worker.connector;

import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * S3-backed {@link KuaiaFileSystem}. URIs are {@code s3://bucket/key} strings; {@link #list(String)}
 * returns children in that same {@code s3://} string space (the SPI contract), so callers relativize
 * by stripping the argument prefix.
 *
 * <p>Keys may contain spaces and other special characters. The {@code s3://bucket/<key>} child
 * strings are built and parsed by string concatenation/splitting only (never {@link URI} encoding),
 * so they round-trip through {@link #parse(String)} unchanged.
 *
 * <p>This class owns the AWS {@link S3Client} (moved here from the former {@code AwsSdkS3ObjectStore})
 * and stays in the engine module so the AWS SDK never leaks into {@code kuaia-connectors}.
 */
public class S3FileSystem implements KuaiaFileSystem {
    private static final String SCHEME = "s3://";

    private final S3Client client;

    /** Public entry point: build the AWS client from source config + process environment. */
    public S3FileSystem(PipelineConfig.SourceConfig config) throws PipelineExecutionException {
        this(config, System.getenv());
    }

    /** Env-injection seam for tests that resolve credentials without touching {@code System.getenv()}. */
    S3FileSystem(PipelineConfig.SourceConfig config, Map<String, String> environment)
            throws PipelineExecutionException {
        this(buildClient(config, environment));
    }

    /** Client-injection seam for unit tests: no live network, no credential resolution. */
    S3FileSystem(S3Client client) {
        this.client = client;
    }

    private static S3Client buildClient(PipelineConfig.SourceConfig config, Map<String, String> environment)
            throws PipelineExecutionException {
        String accessKey = requireEnv(config.getAccessKeyEnv(), environment, "S3 access key");
        String secretKey = requireEnv(config.getSecretKeyEnv(), environment, "S3 secret key");
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(config.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(config.isPathStyleAccess())
                        .build());
        if (hasText(config.getEndpoint())) {
            builder.endpointOverride(URI.create(config.getEndpoint()));
        }
        return builder.build();
    }

    @Override
    public boolean exists(String uri) throws PipelineExecutionException {
        S3Location location = parse(uri);
        if (location.key.isEmpty() || location.key.endsWith("/")) {
            // Prefix: "exists" iff the listing has at least one object under it. Keep it to one call.
            try {
                ListObjectsV2Request request = ListObjectsV2Request.builder()
                        .bucket(location.bucket)
                        .prefix(location.key)
                        .maxKeys(1)
                        .build();
                return !client.listObjectsV2(request).contents().isEmpty();
            } catch (SdkException e) {
                throw new PipelineExecutionException("S3 source list failed: " + e.getMessage(), e);
            }
        }
        try {
            client.headObject(HeadObjectRequest.builder().bucket(location.bucket).key(location.key).build());
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (SdkException e) {
            throw new PipelineExecutionException(
                    "S3 source read failed at " + location.key + ": " + e.getMessage(), e);
        }
    }

    /**
     * Trailing-slash convention: an {@code s3://} URI ending in {@code /} denotes a prefix
     * (directory); anything else is treated as a single object key.
     */
    @Override
    public boolean isDirectory(String uri) {
        return uri.endsWith("/");
    }

    @Override
    public List<String> list(String uri) throws PipelineExecutionException {
        S3Location location = parse(uri);
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(location.bucket)
                    .prefix(location.key)
                    .build();
            ListObjectsV2Iterable pages = client.listObjectsV2Paginator(request);
            List<String> children = new ArrayList<>();
            for (S3Object object : pages.contents()) {
                String key = object.key();
                if (key.endsWith("/")) {
                    // Skip pseudo-directory markers (zero-byte keys ending in "/").
                    continue;
                }
                children.add(SCHEME + location.bucket + "/" + key);
            }
            return children;
        } catch (SdkException e) {
            throw new PipelineExecutionException("S3 source list failed: " + e.getMessage(), e);
        }
    }

    @Override
    public byte[] readAllBytes(String uri) throws PipelineExecutionException {
        S3Location location = parse(uri);
        try {
            return client.getObjectAsBytes(GetObjectRequest.builder()
                    .bucket(location.bucket)
                    .key(location.key)
                    .build()).asByteArray();
        } catch (SdkException e) {
            throw new PipelineExecutionException(
                    "S3 source read failed at " + location.key + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    /** Split {@code s3://bucket/key} into its bucket and key. The key may be empty (bucket root). */
    static S3Location parse(String uri) {
        if (uri == null || !uri.startsWith(SCHEME)) {
            throw new IllegalArgumentException("Not an s3:// URI: " + uri);
        }
        String rest = uri.substring(SCHEME.length());
        int slash = rest.indexOf('/');
        if (slash < 0) {
            return new S3Location(rest, "");
        }
        return new S3Location(rest.substring(0, slash), rest.substring(slash + 1));
    }

    static final class S3Location {
        final String bucket;
        final String key;

        S3Location(String bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }
    }

    private static String requireEnv(String envName, Map<String, String> environment, String label)
            throws PipelineExecutionException {
        String value = envName == null ? null : environment.get(envName);
        if (!hasText(value)) {
            throw new PipelineExecutionException("Missing " + label + " environment variable: " + envName);
        }
        return value;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
