package com.kuaia.engine;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.pipeline.LocalPipelineCheckpointStore;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.pipeline.PipelineRunSummary;
import com.kuaia.engine.worker.connector.ConsoleSink;
import com.kuaia.engine.worker.connector.FakeSource;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.transform.TransformPipeline;
import com.kuaia.engine.worker.connector.FileSource;
import com.kuaia.engine.worker.connector.FileSink;
import com.kuaia.engine.worker.connector.MockVectorSink;

import java.io.PrintStream;
import java.nio.file.Paths;

public class LocalPipelineRunner {
    public int run(PrintStream out) throws Exception {
        FakeSource source = new FakeSource();
        ConsoleSink sink = new ConsoleSink(source.getRowType(), out);

        source.open();
        sink.open();
        int rows = 0;
        try {
            out.println("Starting Local Pipeline...");
            for (int i = 0; i < 10; i++) {
                source.pollNext(sink::write);
                rows++;
            }
            out.println("Pipeline Finished. rows=" + rows);
            return rows;
        } finally {
            source.close();
            sink.close();
        }
    }

    public PipelineRunSummary run(PipelineConfig config, PrintStream out) throws Exception {
        long startedAt = System.nanoTime();
        PipelineRunSummary summary;
        if (hasCheckpointStateDir(config)) {
            summary = runWithCheckpoint(config, out);
        } else {
            summary = runWithoutCheckpoint(config, out);
        }
        long durationMillis = (System.nanoTime() - startedAt) / 1_000_000L;
        return summary.withDurationMillis(durationMillis);
    }

    private PipelineRunSummary runWithoutCheckpoint(PipelineConfig config, PrintStream out) throws Exception {
        FileSource source = new FileSource(Paths.get(config.getSource().getPath()));
        source.open();
        SinkWriter sink = null;
        try {
            TransformPipeline transforms = TransformPipeline.from(source.getRowType(), config.getTransforms());
            sink = createSink(config, transforms.getOutputType(), out);
            sink.open();
            out.println("Starting pipeline: " + config.getName());
            SinkWriter openedSink = sink;
            int rows = source.readFrom(0L, (seqId, row) -> openedSink.write(transforms.apply(row)));
            out.println("Pipeline Finished. rows=" + rows);
            return new PipelineRunSummary(rows, rows, 0L, rows, TaskState.COMPLETED, 0L);
        } finally {
            source.close();
            if (sink != null) {
                sink.close();
            }
        }
    }

    private PipelineRunSummary runWithCheckpoint(PipelineConfig config, PrintStream out) throws Exception {
        FileSource source = new FileSource(Paths.get(config.getSource().getPath()));
        source.open();
        SinkWriter sink = null;
        try {
            TransformPipeline transforms = TransformPipeline.from(source.getRowType(), config.getTransforms());
            out.println("Starting pipeline: " + config.getName());
            try (LocalPipelineCheckpointStore checkpointStore = new LocalPipelineCheckpointStore(
                    Paths.get(config.getCheckpoint().getStateDir()),
                    config.getName())) {
                TaskRecord task = checkpointStore.startOrResume();
                if (task.getState() == TaskState.COMPLETED) {
                    out.println("Pipeline Finished. rows=0 checkpoint="
                            + task.getLastCheckpointSeq()
                            + " state="
                            + task.getState());
                    return new PipelineRunSummary(
                            0L,
                            0L,
                            task.getLastCheckpointSeq(),
                            task.getLastCheckpointSeq(),
                            task.getState(),
                            0L);
                }

                sink = createSink(config, transforms.getOutputType(), out);
                sink.open();
                SinkWriter openedSink = sink;
                final TaskRecord[] taskRef = new TaskRecord[]{task};
                long rowsSkipped = task.getLastCheckpointSeq();
                int rows = source.readFrom(task.getLastCheckpointSeq(), (seqId, row) -> {
                    openedSink.write(transforms.apply(row));
                    taskRef[0] = checkpointStore.checkpoint(taskRef[0], seqId);
                });
                TaskRecord completed = checkpointStore.complete(taskRef[0]);
                out.println("Pipeline Finished. rows="
                        + rows
                        + " checkpoint="
                        + completed.getLastCheckpointSeq()
                        + " state="
                        + completed.getState());
                return new PipelineRunSummary(
                        rows,
                        rows,
                        rowsSkipped,
                        completed.getLastCheckpointSeq(),
                        completed.getState(),
                        0L);
            }
        } finally {
            source.close();
            if (sink != null) {
                sink.close();
            }
        }
    }

    private boolean hasCheckpointStateDir(PipelineConfig config) {
        String stateDir = config.getCheckpoint().getStateDir();
        return stateDir != null && !stateDir.trim().isEmpty();
    }

    private SinkWriter createSink(PipelineConfig config, KuaiaRowType rowType, PrintStream out)
            throws PipelineExecutionException {
        String sinkType = config.getSink().getType();
        if ("console".equals(sinkType)) {
            return new ConsoleSink(rowType, out);
        }
        if ("mock-vector".equals(sinkType)) {
            return new MockVectorSink(rowType, out);
        }
        if ("file".equals(sinkType)) {
            return new FileSink(
                    rowType,
                    Paths.get(config.getSink().getPath()),
                    config.getSink().getMode());
        }
        throw new PipelineExecutionException("Unsupported sink.type: " + sinkType);
    }
}
