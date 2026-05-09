package com.kuaia.engine;

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
}
