package com.kuaia.engine.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineConfigTest {
    @Test
    void sourceConfigAllowsUnsetMaxRowsPerSplit() {
        PipelineConfig.SourceConfig source = new PipelineConfig.SourceConfig("file", "data.csv", "csv");

        assertEquals(0, source.getMaxRowsPerSplit());
    }

    @Test
    void sourceConfigRejectsNegativeMaxRowsPerSplit() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> new PipelineConfig.SourceConfig("file", "data.csv", "csv", -1));

        assertEquals("maxRowsPerSplit must not be negative", error.getMessage());
    }
}
