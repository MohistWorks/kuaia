package com.kuaia.engine.worker.connector;

import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3FileSystemTest {
    @Test
    void parsesBucketAndKey() {
        S3FileSystem.S3Location location = S3FileSystem.parse("s3://kuaia-docs/docs/a.md");
        assertEquals("kuaia-docs", location.bucket);
        assertEquals("docs/a.md", location.key);
    }

    @Test
    void parsesBucketRootWithoutKey() {
        S3FileSystem.S3Location location = S3FileSystem.parse("s3://kuaia-docs");
        assertEquals("kuaia-docs", location.bucket);
        assertEquals("", location.key);
    }

    @Test
    void parsesKeyContainingSpaces() {
        S3FileSystem.S3Location location = S3FileSystem.parse("s3://kuaia-docs/reports/q1 summary.md");
        assertEquals("kuaia-docs", location.bucket);
        assertEquals("reports/q1 summary.md", location.key);
    }

    @Test
    void rejectsNonS3Uri() {
        assertThrows(IllegalArgumentException.class, () -> S3FileSystem.parse("file:///tmp/a.md"));
    }

    @Test
    void listReturnsChildrenInS3SpaceSkippingDirectoryMarkers() throws Exception {
        S3Client client = mock(S3Client.class);
        // The listing includes two pseudo-directory markers (keys ending in "/") that must be skipped,
        // and a key with a space that must round-trip through the s3:// child string unchanged.
        List<S3Object> listed = Arrays.asList(
                S3Object.builder().key("docs/").build(),
                S3Object.builder().key("docs/a.md").build(),
                S3Object.builder().key("docs/sub/").build(),
                S3Object.builder().key("docs/q1 summary.txt").build());
        // contents() is a final method on the paginator, so mock the underlying single-page call and
        // return a REAL ListObjectsV2Iterable (its public ctor + the stubbed listObjectsV2 drive it).
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder().contents(listed).isTruncated(false).build());
        when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class)))
                .thenAnswer(inv -> new ListObjectsV2Iterable(client, inv.getArgument(0)));

        S3FileSystem fs = new S3FileSystem(client);
        List<String> children = fs.list("s3://kuaia-docs/docs/");

        assertEquals(
                Arrays.asList("s3://kuaia-docs/docs/a.md", "s3://kuaia-docs/docs/q1 summary.txt"),
                children);

        ArgumentCaptor<ListObjectsV2Request> request = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(client).listObjectsV2Paginator(request.capture());
        assertEquals("kuaia-docs", request.getValue().bucket());
        assertEquals("docs/", request.getValue().prefix());
    }

    @Test
    void listWrapsSdkFailureWithReusedMessage() {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class)))
                .thenThrow(NoSuchKeyException.builder().message("boom").build());

        S3FileSystem fs = new S3FileSystem(client);
        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class, () -> fs.list("s3://kuaia-docs/docs/"));

        assertTrue(error.getMessage().startsWith("S3 source list failed: "), error.getMessage());
    }

    @Test
    void readAllBytesReturnsObjectBytesForKeyWithSpace() throws Exception {
        S3Client client = mock(S3Client.class);
        byte[] data = "Alpha".getBytes(StandardCharsets.UTF_8);
        when(client.getObjectAsBytes(any(GetObjectRequest.class)))
                .thenReturn(ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), data));

        S3FileSystem fs = new S3FileSystem(client);
        byte[] read = fs.readAllBytes("s3://kuaia-docs/docs/q1 summary.txt");

        assertArrayEquals(data, read);
        ArgumentCaptor<GetObjectRequest> request = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(client).getObjectAsBytes(request.capture());
        assertEquals("kuaia-docs", request.getValue().bucket());
        assertEquals("docs/q1 summary.txt", request.getValue().key());
    }

    @Test
    void readAllBytesWrapsSdkFailureWithReusedMessage() {
        S3Client client = mock(S3Client.class);
        when(client.getObjectAsBytes(any(GetObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("gone").build());

        S3FileSystem fs = new S3FileSystem(client);
        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class, () -> fs.readAllBytes("s3://kuaia-docs/docs/a.md"));

        assertTrue(error.getMessage().startsWith("S3 source read failed at docs/a.md: "), error.getMessage());
    }

    @Test
    void existsReturnsTrueForPresentObject() throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(HeadObjectResponse.builder().build());

        S3FileSystem fs = new S3FileSystem(client);

        assertTrue(fs.exists("s3://kuaia-docs/docs/a.md"));
    }

    @Test
    void existsReturnsFalseForAbsentObject() throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().build());

        S3FileSystem fs = new S3FileSystem(client);

        assertFalse(fs.exists("s3://kuaia-docs/docs/missing.md"));
    }

    @Test
    void existsChecksListingForPrefix() throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder()
                        .contents(S3Object.builder().key("docs/a.md").build())
                        .build());

        S3FileSystem fs = new S3FileSystem(client);

        assertTrue(fs.exists("s3://kuaia-docs/docs/"));
    }

    @Test
    void isDirectoryFollowsTrailingSlashConvention() {
        S3FileSystem fs = new S3FileSystem(mock(S3Client.class));
        assertTrue(fs.isDirectory("s3://kuaia-docs/docs/"));
        assertFalse(fs.isDirectory("s3://kuaia-docs/docs/a.md"));
    }

    @Test
    void missingAccessKeyEnvIsReported() {
        PipelineConfig.SourceConfig config = new PipelineConfig.SourceConfig(
                "file", "s3://kuaia-docs/docs/", "document", "auto",
                "http://127.0.0.1:9000", "us-east-1", "KUAIA_S3_ACCESS_KEY", "KUAIA_S3_SECRET_KEY", true);

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class, () -> new S3FileSystem(config, Map.of()));

        assertEquals("Missing S3 access key environment variable: KUAIA_S3_ACCESS_KEY", error.getMessage());
    }

    @Test
    void missingSecretKeyEnvIsReported() {
        PipelineConfig.SourceConfig config = new PipelineConfig.SourceConfig(
                "file", "s3://kuaia-docs/docs/", "document", "auto",
                "http://127.0.0.1:9000", "us-east-1", "KUAIA_S3_ACCESS_KEY", "KUAIA_S3_SECRET_KEY", true);

        PipelineExecutionException error = assertThrows(
                PipelineExecutionException.class,
                () -> new S3FileSystem(config, Map.of("KUAIA_S3_ACCESS_KEY", "ak")));

        assertEquals("Missing S3 secret key environment variable: KUAIA_S3_SECRET_KEY", error.getMessage());
    }

    @Test
    void buildsClientWithEndpointOverride() throws Exception {
        // Exercises the endpoint-override branch and close() without any live network call: no S3
        // operation is issued, so the client is only constructed and released.
        PipelineConfig.SourceConfig config = new PipelineConfig.SourceConfig(
                "file", "s3://kuaia-docs/docs/", "csv", null,
                "http://127.0.0.1:9000", "us-east-1", "KUAIA_S3_ACCESS_KEY", "KUAIA_S3_SECRET_KEY", true);
        Map<String, String> env = Map.of("KUAIA_S3_ACCESS_KEY", "ak", "KUAIA_S3_SECRET_KEY", "sk");

        S3FileSystem fs = new S3FileSystem(config, env);
        fs.close();
    }
}
