package com.kuaia.engine.worker.connector.v2;

import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.worker.connector.FileSource;

import java.util.Collections;
import java.util.List;

public class FileSourceAdapter implements SourceEnumerator {
    private final FileSource source;
    private final String splitIdPrefix;
    private final int maxRowsPerSplit;
    private List<SourceSplit> splits;

    public FileSourceAdapter(FileSource source, String splitIdPrefix, int maxRowsPerSplit) {
        if (splitIdPrefix == null || splitIdPrefix.trim().isEmpty()) {
            throw new IllegalArgumentException("splitIdPrefix must not be empty");
        }
        if (maxRowsPerSplit <= 0) {
            throw new IllegalArgumentException("maxRowsPerSplit must be greater than zero");
        }
        this.source = source;
        this.splitIdPrefix = splitIdPrefix;
        this.maxRowsPerSplit = maxRowsPerSplit;
    }

    @Override
    public void open() throws Exception {
        source.open();
        long recordCount = source.getRecordCount();
        if (recordCount == 0L) {
            splits = Collections.singletonList(new SourceSplit(splitIdPrefix));
            return;
        }
        splits = SourceSplitPlanner.planFixedSize(splitIdPrefix, recordCount, maxRowsPerSplit);
    }

    @Override
    public List<SourceSplit> enumerateSplits() throws Exception {
        ensureOpen();
        return splits;
    }

    @Override
    public BatchSourceReader createReader(SourceSplit split) throws Exception {
        ensureOpen();
        return new LocalSourceAdapter(source, splits).createReader(split);
    }

    @Override
    public KuaiaRowType getRowType() {
        return source.getRowType();
    }

    @Override
    public void close() throws Exception {
        source.close();
    }

    private void ensureOpen() throws PipelineExecutionException {
        if (splits == null) {
            throw new PipelineExecutionException("File source adapter is not open");
        }
    }
}
