package com.kuaia.engine.worker.connector;

import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.pipeline.PipelineExecutionException;

import java.util.Collections;

public class DuckDBSource extends JdbcQuerySource {
    public DuckDBSource(PipelineConfig.SourceConfig config) throws PipelineExecutionException {
        super(config, Collections.emptyMap(), "DuckDB", false);
    }
}
