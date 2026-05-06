package com.kuaia.common.data;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class BinaryRowTest {
    @Test
    public void testSetGetLong() {
        BinaryRow row = new BinaryRow(2);
        row.setLong(0, 123456789L);
        row.setLong(1, 987654321L);
        assertEquals(123456789L, row.getLong(0));
        assertEquals(987654321L, row.getLong(1));
    }
}
