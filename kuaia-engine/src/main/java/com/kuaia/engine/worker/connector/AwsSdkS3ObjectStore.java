package com.kuaia.engine.worker.connector;

import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class AwsSdkS3ObjectStore implements S3ObjectStore {
    private final S3Client client;

    AwsSdkS3ObjectStore(PipelineConfig.SourceConfig config) throws PipelineExecutionException {
        this(config, System.getenv());
    }

    AwsSdkS3ObjectStore(PipelineConfig.SourceConfig config, Map<String, String> environment)
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
        this.client = builder.build();
    }

    @Override
    public List<S3ObjectMetadata> listObjects(String bucket, String prefix) throws PipelineExecutionException {
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(prefix == null ? "" : prefix)
                    .build();
            ListObjectsV2Iterable pages = client.listObjectsV2Paginator(request);
            List<S3ObjectMetadata> objects = new ArrayList<>();
            for (S3Object object : pages.contents()) {
                objects.add(new S3ObjectMetadata(object.key(), object.size()));
            }
            return objects;
        } catch (SdkException e) {
            throw new PipelineExecutionException("S3 source list failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String readUtf8Object(String bucket, String key) throws PipelineExecutionException {
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            ResponseBytes<GetObjectResponse> bytes = client.getObjectAsBytes(request);
            return new String(bytes.asByteArray(), StandardCharsets.UTF_8);
        } catch (SdkException e) {
            throw new PipelineExecutionException("S3 source read failed at " + key + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private String requireEnv(String envName, Map<String, String> environment, String label)
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
