package com.kuaia.engine;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.engine.pipeline.LocalPipelineCheckpointStore;
import com.kuaia.engine.worker.connector.ConsoleSink;
import com.kuaia.engine.worker.connector.FakeSource;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.worker.connector.FileSource;

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
        ConsoleSink sink = new ConsoleSink(source.getRowType(), out);
        sink.open();
        try {
            out.println("Starting pipeline: " + config.getName());
            int rows = source.readAll(sink);
            out.println("Pipeline Finished. rows=" + rows);
            return rows;
        } finally {
            source.close();
            sink.close();
        }
    }

    private int runWithCheckpoint(PipelineConfig config, PrintStream out) throws Exception {
        FileSource source = new FileSource(Paths.get(config.getSource().getPath()));
        source.open();
        ConsoleSink sink = new ConsoleSink(source.getRowType(), out);
        sink.open();
        try (LocalPipelineCheckpointStore checkpointStore = new LocalPipelineCheckpointStore(
                Paths.get(config.getCheckpoint().getStateDir()),
                config.getName())) {
            out.println("Starting pipeline: " + config.getName());
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
                sink.write(row);
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
        } finally {
            source.close();
            sink.close();
        }
    }

    private boolean hasCheckpointStateDir(PipelineConfig config) {
        String stateDir = config.getCheckpoint().getStateDir();
        return stateDir != null && !stateDir.trim().isEmpty();
    }
}
