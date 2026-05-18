package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class S3ObjectSourceTest {
    @Test
    void readsSupportedObjectsInStableKeyOrder() throws Exception {
        FakeObjectStore store = new FakeObjectStore(
                Arrays.asList(
                        new S3ObjectMetadata("docs/b.md", 4L),
                        new S3ObjectMetadata("docs/image.png", 7L),
                        new S3ObjectMetadata("docs/a.txt", 5L),
                        new S3ObjectMetadata("docs/folder/", 0L)),
                Arrays.asList(
                        object("docs/a.txt", "Alpha"),
                        object("docs/b.md", "Beta")));

        S3ObjectSource source = new S3ObjectSource("kuaia-docs", "docs/", store);
        source.open();
        List<Long> seqIds = new ArrayList<>();
        List<BinaryRow> rows = new ArrayList<>();

        int read = source.readFrom(
                0L,
                (seqId, row) -> {
                    seqIds.add(seqId);
                    rows.add(row);
                },
                (seqId, error) -> {
                    throw new AssertionError("Unexpected row error for seq " + seqId, error);
                });

        assertEquals(2, read);
        assertArrayEquals(new String[]{"id", "key", "content"}, source.getRowType().getFieldNames());
        assertArrayEquals(
                new DataType[]{DataType.LONG, DataType.STRING, DataType.STRING},
                source.getRowType().getFieldTypes());
        assertEquals(Arrays.asList(1L, 2L), seqIds);
        assertEquals(1L, rows.get(0).getLong(0));
        assertEquals("docs/a.txt", rows.get(0).getString(1));
        assertEquals("Alpha", rows.get(0).getString(2));
        assertEquals(2L, rows.get(1).getLong(0));
        assertEquals("docs/b.md", rows.get(1).getString(1));
        assertEquals("Beta", rows.get(1).getString(2));
        source.close();
        assertEquals(true, store.closed);
    }

    @Test
    void rejectsPrefixWithoutSupportedObjects() {
        FakeObjectStore store = new FakeObjectStore(
                Arrays.asList(new S3ObjectMetadata("docs/image.png", 7L)),
                new ArrayList<>());
        S3ObjectSource source = new S3ObjectSource("kuaia-docs", "docs/", store);

        PipelineExecutionException error = assertThrows(PipelineExecutionException.class, source::open);

        assertEquals("S3 source has no supported objects: s3://kuaia-docs/docs/", error.getMessage());
    }

    private static FakeObject object(String key, String content) {
        return new FakeObject(key, content);
    }

    private static final class FakeObjectStore implements S3ObjectStore {
        private final List<S3ObjectMetadata> objects;
        private final List<FakeObject> contents;
        private boolean closed;

        private FakeObjectStore(List<S3ObjectMetadata> objects, List<FakeObject> contents) {
            this.objects = objects;
            this.contents = contents;
        }

        @Override
        public List<S3ObjectMetadata> listObjects(String bucket, String prefix) {
            assertEquals("kuaia-docs", bucket);
            assertEquals("docs/", prefix);
            return objects;
        }

        @Override
        public String readUtf8Object(String bucket, String key) throws PipelineExecutionException {
            assertEquals("kuaia-docs", bucket);
            for (FakeObject object : contents) {
                if (object.key.equals(key)) {
                    return object.content;
                }
            }
            throw new PipelineExecutionException("missing fake object " + key);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class FakeObject {
        private final String key;
        private final String content;

        private FakeObject(String key, String content) {
            this.key = key;
            this.content = content;
        }
    }
}
