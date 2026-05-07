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

    @Test
    public void testSetGetString() {
        BinaryRow row = new BinaryRow(2);
        row.setLong(0, 1L);
        row.setString(1, "Hello Kuaia");
        assertEquals(1L, row.getLong(0));
        assertEquals("Hello Kuaia", row.getString(1));
    }

    @Test
    public void testSetGetVector() {
        BinaryRow row = new BinaryRow(1);
        float[] vector = new float[]{0.1f, -0.2f, 3.14f};
        row.setVector(0, vector);
        assertArrayEquals(vector, row.getVector(0));
    }
}
