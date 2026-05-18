package com.kuaia.engine.worker.connector;

import com.kuaia.common.api.SinkWriter;
import com.kuaia.common.type.KuaiaRowType;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SinkFactoryRegistry {
    private final Map<String, VectorSinkFactory> factories;

    public SinkFactoryRegistry(Map<String, VectorSinkFactory> factories) {
        this.factories = Collections.unmodifiableMap(new HashMap<>(factories));
    }

    public static SinkFactoryRegistry defaultRegistry() {
        Map<String, VectorSinkFactory> factories = new HashMap<>();
        factories.put("mock-vector", new MockVectorSinkFactory());
        factories.put("qdrant", new QdrantVectorSinkFactory());
        factories.put("pgvector", new PgvectorVectorSinkFactory());
        return new SinkFactoryRegistry(factories);
    }

    public SinkWriter create(String sinkType, KuaiaRowType rowType, PrintStream out)
            throws PipelineExecutionException {
        return create(sinkType, rowType, out, null);
    }

    public SinkWriter create(
            String sinkType,
            KuaiaRowType rowType,
            PrintStream out,
            PipelineConfig.SinkConfig config)
            throws PipelineExecutionException {
        VectorSinkFactory factory = factories.get(sinkType);
        if (factory == null) {
            throw new PipelineExecutionException("Unsupported sink.type: " + sinkType);
        }
        return factory.create(rowType, out, config);
    }
}
