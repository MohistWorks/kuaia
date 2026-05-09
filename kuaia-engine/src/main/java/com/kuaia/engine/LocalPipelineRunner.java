package com.kuaia.engine;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.pipeline.LocalPipelineCheckpointStore;
import com.kuaia.engine.pipeline.PipelineExecutionException;
import com.kuaia.engine.worker.connector.ConsoleSink;
import com.kuaia.engine.worker.connector.FakeSource;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.transform.TransformPipeline;
import com.kuaia.engine.worker.connector.FileSource;
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

    public int run(PipelineConfig config, PrintStream out) throws Exception {
        if (hasCheckpointStateDir(config)) {
            return runWithCheckpoint(config, out);
        }
        return runWithoutCheckpoint(config, out);
    }

    private int runWithoutCheckpoint(PipelineConfig config, PrintStream out) throws Exception {
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
            return rows;
        } finally {
            source.close();
            if (sink != null) {
                sink.close();
            }
        }
    }

    private int runWithCheckpoint(PipelineConfig config, PrintStream out) throws Exception {
        FileSource source = new FileSource(Paths.get(config.getSource().getPath()));
        source.open();
        SinkWriter sink = null;
        try {
            TransformPipeline transforms = TransformPipeline.from(source.getRowType(), config.getTransforms());
            sink = createSink(config, transforms.getOutputType(), out);
            sink.open();
            out.println("Starting pipeline: " + config.getName());
            SinkWriter openedSink = sink;
            try (LocalPipelineCheckpointStore checkpointStore = new LocalPipelineCheckpointStore(
                    Paths.get(config.getCheckpoint().getStateDir()),
                    config.getName())) {
                TaskRecord task = checkpointStore.startOrResume();
                if (task.getState() == TaskState.COMPLETED) {
                    out.println("Pipeline Finished. rows=0 checkpoint="
                            + task.getLastCheckpointSeq()
                            + " state="
                            + task.getState());
                    return 0;
                }

                final TaskRecord[] taskRef = new TaskRecord[]{task};
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
                return rows;
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
        throw new PipelineExecutionException("Unsupported sink.type: " + sinkType);
    }
}
