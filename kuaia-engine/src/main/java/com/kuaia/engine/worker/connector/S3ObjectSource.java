package com.kuaia.engine.worker.connector;

import com.kuaia.common.data.BinaryRow;
import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

public class S3ObjectSource implements LocalSource {
    private static final KuaiaRowType ROW_TYPE = new KuaiaRowType(
            new String[]{"id", "key", "content"},
            new DataType[]{DataType.LONG, DataType.STRING, DataType.STRING});

    private final String bucket;
    private final String prefix;
    private final S3ObjectStore store;
    private List<S3ObjectMetadata> objects;

    public S3ObjectSource(PipelineConfig.SourceConfig config) throws PipelineExecutionException {
        this(config.getBucket(), config.getPrefix(), new AwsSdkS3ObjectStore(config));
    }

    S3ObjectSource(String bucket, String prefix, S3ObjectStore store) {
        this.bucket = bucket;
        this.prefix = prefix == null ? "" : prefix;
        this.store = store;
    }

    @Override
    public void open() throws PipelineExecutionException {
        List<S3ObjectMetadata> listed = store.listObjects(bucket, prefix);
        objects = new ArrayList<>();
        for (S3ObjectMetadata object : listed) {
            if (isSupportedObject(object)) {
                objects.add(object);
            }
        }
        objects.sort(Comparator.comparing(S3ObjectMetadata::getKey));
        if (objects.isEmpty()) {
            throw new PipelineExecutionException("S3 source has no supported objects: s3://" + bucket + "/" + prefix);
        }
    }

    @Override
    public int readFrom(long lastCheckpointSeq, RecordConsumer consumer, RecordErrorConsumer errorConsumer)
            throws Exception {
        ensureOpen();
        int count = 0;
        for (int i = 0; i < objects.size(); i++) {
            long seqId = i + 1L;
            if (seqId <= lastCheckpointSeq) {
                continue;
            }
            S3ObjectMetadata object = objects.get(i);
            BinaryRow row;
            try {
                row = readObject(seqId, object.getKey());
            } catch (PipelineExecutionException e) {
                if (errorConsumer.accept(seqId, e)) {
                    continue;
                }
                throw e;
            }
            consumer.accept(seqId, row);
            count++;
        }
        return count;
    }

    @Override
    public KuaiaRowType getRowType() {
        return ROW_TYPE;
    }

    @Override
    public void close() throws Exception {
        objects = null;
        store.close();
    }

    private BinaryRow readObject(long seqId, String key) throws PipelineExecutionException {
        String content = store.readUtf8Object(bucket, key);
        BinaryRow row = new BinaryRow(3);
        row.setLong(0, seqId);
        row.setString(1, key);
        row.setString(2, content);
        return row;
    }

    private boolean isSupportedObject(S3ObjectMetadata object) {
        String key = object.getKey();
        if (key == null || key.endsWith("/")) {
            return false;
        }
        String name = key.toLowerCase(Locale.ROOT);
        return name.endsWith(".txt")
                || name.endsWith(".md")
                || name.endsWith(".markdown")
                || name.endsWith(".jsonl")
                || name.endsWith(".csv");
    }

    private void ensureOpen() throws PipelineExecutionException {
        if (objects == null) {
            throw new PipelineExecutionException("S3 source is not open");
        }
    }
}
