package com.kuaia.engine.worker.connector;

import com.kuaia.common.type.DataType;
import com.kuaia.common.type.KuaiaRowType;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The vector-sink effectively-once guarantee rests on a stable id: a row type without the required id
 * field is rejected at construction, so a sink can never silently invent ids that would duplicate on
 * replay. (Deduplication itself is provided by the store's upsert-by-id, covered by each sink's own
 * upsert tests.)
 */
class VectorSinkIdRequiredTest {

    @Test
    void mockVectorSinkRejectsRowTypeWithoutId() {
        KuaiaRowType noId = new KuaiaRowType(
                new String[] {"embedding"}, new DataType[] {DataType.VECTOR});
        assertThrows(Exception.class,
                () -> new MockVectorSink(noId, new PrintStream(OutputStream.nullOutputStream())));
    }
}
