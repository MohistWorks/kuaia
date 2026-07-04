package com.kuaia.engine.pipeline;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.worker.connector.ConsoleSink;
import com.kuaia.engine.worker.connector.DocumentDirectorySource;
import com.kuaia.engine.worker.connector.DuckDBSource;
import com.kuaia.engine.worker.connector.FileSink;
import com.kuaia.engine.worker.connector.FileSource;
import com.kuaia.engine.worker.connector.MySQLSource;
import com.kuaia.engine.worker.connector.PostgresSource;
import com.kuaia.engine.worker.connector.S3ObjectSource;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import com.kuaia.engine.worker.connector.v2.BatchSinkWriter;
import com.kuaia.engine.worker.connector.v2.FileSourceAdapter;
import com.kuaia.engine.worker.connector.v2.LocalSourceAdapter;
import com.kuaia.engine.worker.connector.v2.SinkWriterBatchAdapter;
import com.kuaia.engine.worker.connector.v2.SourceEnumerator;

import java.io.PrintStream;
import java.nio.file.Paths;

/** 从 PipelineConfig 装配连接器；本地 runner 与 worker 共用。 */
public class ConnectorFactory {
    private static final int DEFAULT_FILE_ROWS_PER_SPLIT = 10_000;

    private final SinkFactoryRegistry sinkFactories;
    private final int fileRowsPerSplit;

    public ConnectorFactory(SinkFactoryRegistry sinkFactories) {
        this(sinkFactories, DEFAULT_FILE_ROWS_PER_SPLIT);
    }

    public ConnectorFactory(SinkFactoryRegistry sinkFactories, int fileRowsPerSplit) {
        if (fileRowsPerSplit <= 0) {
            throw new IllegalArgumentException("fileRowsPerSplit must be greater than zero");
        }
        this.sinkFactories = sinkFactories;
        this.fileRowsPerSplit = fileRowsPerSplit;
    }

    public SourceEnumerator createSource(PipelineConfig config) throws PipelineExecutionException {
        String sourceType = config.getSource().getType();
        if ("file".equals(sourceType)) {
            return new FileSourceAdapter(
                    new FileSource(Paths.get(config.getSource().getPath()), config.getSource().getFormat()),
                    "file-0",
                    fileRowsPerSplit(config));
        }
        if ("postgres".equals(sourceType)) {
            return new LocalSourceAdapter(new PostgresSource(config.getSource()), "postgres-0");
        }
        if ("mysql".equals(sourceType)) {
            return new LocalSourceAdapter(new MySQLSource(config.getSource()), "mysql-0");
        }
        if ("duckdb".equals(sourceType)) {
            return new LocalSourceAdapter(new DuckDBSource(config.getSource()), "duckdb-0");
        }
        if ("document-directory".equals(sourceType)) {
            return new LocalSourceAdapter(
                    new DocumentDirectorySource(Paths.get(config.getSource().getPath())),
                    "document-directory-0");
        }
        if ("s3".equals(sourceType)) {
            return new LocalSourceAdapter(new S3ObjectSource(config.getSource()), "s3-0");
        }
        throw new PipelineExecutionException("Unsupported source.type: " + sourceType);
    }

    public BatchSinkWriter createSink(PipelineConfig config, KuaiaRowType rowType, PrintStream out)
            throws PipelineExecutionException {
        return createSink(config, rowType, out, 1L);
    }

    /**
     * @param resumeFromSeq first sequence id this attempt will (re)write ({@code 1} = fresh run). The
     *                      file sink uses it to truncate its output back to the committed resume point
     *                      so re-delivered rows overwrite rather than duplicate (effectively-once).
     */
    public BatchSinkWriter createSink(PipelineConfig config, KuaiaRowType rowType, PrintStream out, long resumeFromSeq)
            throws PipelineExecutionException {
        String sinkType = config.getSink().getType();
        if ("file".equals(sinkType)) {
            FileSink fileSink = new FileSink(
                    rowType,
                    Paths.get(config.getSink().getPath()),
                    config.getSink().getFormat(),
                    config.getSink().getMode(),
                    resumeFromSeq);
            return new SinkWriterBatchAdapter(fileSink, commit -> fileSink.recordCommit(commit.getMaxSeqId()));
        }
        SinkWriter sink;
        if ("console".equals(sinkType)) {
            sink = new ConsoleSink(rowType, out);
        } else if ("mock-vector".equals(sinkType)
                || "qdrant".equals(sinkType)
                || "pgvector".equals(sinkType)
                || "milvus".equals(sinkType)) {
            sink = sinkFactories.create(sinkType, rowType, out, config.getSink());
        } else {
            throw new PipelineExecutionException("Unsupported sink.type: " + sinkType);
        }
        return new SinkWriterBatchAdapter(sink);
    }

    private int fileRowsPerSplit(PipelineConfig config) {
        int configured = config.getSource().getMaxRowsPerSplit();
        return configured > 0 ? configured : fileRowsPerSplit;
    }
}
