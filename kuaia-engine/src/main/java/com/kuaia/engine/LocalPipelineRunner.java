package com.kuaia.engine;

import com.kuaia.engine.worker.connector.ConsoleSink;
import com.kuaia.engine.worker.connector.FakeSource;

import java.io.PrintStream;

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
}
