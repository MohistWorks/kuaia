package com.kuaia.engine.worker.connector;

import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.Map;

public class PostgresSource extends JdbcQuerySource {
    public PostgresSource(PipelineConfig.SourceConfig config) throws PipelineExecutionException {
        this(config, System.getenv());
    }

    PostgresSource(PipelineConfig.SourceConfig config, Map<String, String> environment)
            throws PipelineExecutionException {
        super(config, environment, "Postgres");
    }
}
