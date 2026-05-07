package com.kuaia.engine;

import com.kuaia.engine.worker.connector.ConsoleSink;
import com.kuaia.engine.worker.connector.FakeSource;

public class LocalPipelineStarter {
    public static void main(String[] args) throws Exception {
        FakeSource source = new FakeSource();
        ConsoleSink sink = new ConsoleSink(source.getRowType());
        
        source.open();
        sink.open();
        
        System.out.println("Starting Local Pipeline...");
        for (int i = 0; i < 10; i++) {
            source.pollNext(sink::write);
        }
        
        source.close();
        sink.close();
        System.out.println("Pipeline Finished.");
    }
}
