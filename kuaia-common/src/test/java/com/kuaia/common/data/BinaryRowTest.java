package com.kuaia.common.data;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BinaryRowTest {
    @Test
    public void computesHeaderBoundaries() {
        assertEquals(8, BinaryRow.calculateNullBitsSizeInBytes(0));
        assertEquals(8, BinaryRow.calculateNullBitsSizeInBytes(1));
        assertEquals(8, BinaryRow.calculateNullBitsSizeInBytes(56));
        assertEquals(16, BinaryRow.calculateNullBitsSizeInBytes(57));
        assertEquals(16, BinaryRow.calculateNullBitsSizeInBytes(120));
        assertEquals(24, BinaryRow.calculateNullBitsSizeInBytes(121));

        assertEquals(8, BinaryRow.calculateFixedSizeInBytes(0));
        assertEquals(16, BinaryRow.calculateFixedSizeInBytes(1));
        assertEquals(456, BinaryRow.calculateFixedSizeInBytes(56));
        assertEquals(472, BinaryRow.calculateFixedSizeInBytes(57));
    }

    @Test
    public void fieldsDefaultToNull() {
        BinaryRow row = new BinaryRow(3);

        assertEquals(3, row.getFieldCount());
        assertTrue(row.isNullAt(0));
        assertTrue(row.isNullAt(1));
        assertTrue(row.isNullAt(2));
        assertNull(row.getString(1));
        assertThrows(IllegalStateException.class, () -> row.getBoolean(0));
        assertThrows(IllegalStateException.class, () -> row.getInt(0));
        assertThrows(IllegalStateException.class, () -> row.getLong(0));
        assertThrows(IllegalStateException.class, () -> row.getFloat(1));
        assertThrows(IllegalStateException.class, () -> row.getDouble(2));
    }

    @Test
    public void roundTripsScalarTypes() {
        BinaryRow row = new BinaryRow(5);

        row.setBoolean(0, true);
        row.setInt(1, 42);
        row.setLong(2, 123456789L);
        row.setFloat(3, 1.25f);
        row.setDouble(4, -3.5d);

        assertTrue(row.getBoolean(0));
        assertEquals(42, row.getInt(1));
        assertEquals(123456789L, row.getLong(2));
        assertEquals(1.25f, row.getFloat(3), 0.000001f);
        assertEquals(-3.5d, row.getDouble(4), 0.000001d);
        for (int i = 0; i < 5; i++) {
            assertFalse(row.isNullAt(i));
        }
    }

    @Test
    public void roundTripsVariableTypes() {
        BinaryRow row = new BinaryRow(3);
        byte[] bytes = new byte[]{1, 2, 3, -1};
        float[] vector = new float[]{0.1f, -0.2f, 3.14f};

        row.setString(0, "Hello Kuaia");
        row.setBytes(1, bytes);
        row.setVector(2, vector);

        assertEquals("Hello Kuaia", row.getString(0));
        assertArrayEquals(bytes, row.getBytes(1));
        assertArrayEquals(vector, row.getVector(2), 0.000001f);
        assertNotSame(bytes, row.getBytes(1));
        assertNotSame(vector, row.getVector(2));
    }

    @Test
    public void vectorPayloadUsesRawFloat32Blocks() {
        BinaryRow row = new BinaryRow(1);
        row.setVector(0, new float[]{1.5f, -2.25f});

        byte[] bytes = row.toBytes();
        ByteBuffer view = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long slot = view.getLong(BinaryRow.calculateNullBitsSizeInBytes(1));
        int offset = (int) (slot >>> 32);
        int size = (int) slot;

        assertEquals(row.getFixedSizeInBytes(), offset);
        assertEquals(2 * Float.BYTES, size);
        assertEquals(row.getFixedSizeInBytes() + 2 * Float.BYTES, row.getSizeInBytes());
        assertEquals(1.5f, view.getFloat(offset), 0.000001f);
        assertEquals(-2.25f, view.getFloat(offset + Float.BYTES), 0.000001f);
    }

    @Test
    public void supportsExplicitNulls() {
        BinaryRow row = new BinaryRow(3);

        row.setLong(0, 99L);
        row.setString(1, "value");
        row.setVector(2, new float[]{1.0f});
        row.setNullAt(0);
        row.setString(1, null);
        row.setVector(2, null);

        assertTrue(row.isNullAt(0));
        assertTrue(row.isNullAt(1));
        assertTrue(row.isNullAt(2));
        assertNull(row.getString(1));
        assertNull(row.getVector(2));
    }

    @Test
    public void growsVariableSectionForLargeValues() {
        BinaryRow row = new BinaryRow(2);
        String large = repeat("x", 4096);

        row.setString(0, large);
        row.setString(1, large);

        assertEquals(large, row.getString(0));
        assertEquals(large, row.getString(1));
    }

    @Test
    public void usesExplicitLittleEndianOrder() {
        BinaryRow row = new BinaryRow(2);
        row.setLong(0, 0x0102030405060708L);
        row.setFloat(1, 1.5f);

        byte[] bytes = row.toBytes();
        ByteBuffer view = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(0x0102030405060708L, view.getLong(BinaryRow.calculateNullBitsSizeInBytes(2)));
        assertEquals(1.5f, view.getFloat(BinaryRow.calculateNullBitsSizeInBytes(2) + Long.BYTES), 0.000001f);
    }

    @Test
    public void copiesAndSerializesUsedBytesOnly() {
        BinaryRow row = new BinaryRow(2);
        row.setString(0, "alpha");
        row.setLong(1, 7L);

        byte[] bytes = row.toBytes();
        BinaryRow restored = BinaryRow.fromBytes(2, bytes);
        BinaryRow copied = row.copy();

        assertEquals(row.getSizeInBytes(), bytes.length);
        assertEquals("alpha", restored.getString(0));
        assertEquals(7L, restored.getLong(1));
        assertEquals("alpha", copied.getString(0));
        assertEquals(7L, copied.getLong(1));
    }

    @Test
    public void rejectsCorruptedVariableFieldBounds() {
        BinaryRow row = new BinaryRow(1);
        row.setString(0, "alpha");
        byte[] bytes = row.toBytes();
        ByteBuffer view = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        view.putLong(BinaryRow.calculateNullBitsSizeInBytes(1), ((long) Integer.MAX_VALUE << 32) | 16L);

        BinaryRow corrupted = BinaryRow.fromBytes(1, bytes);

        assertThrows(IllegalStateException.class, () -> corrupted.getString(0));
    }

    @Test
    public void rejectsCorruptedVectorPayloadLength() {
        BinaryRow row = new BinaryRow(1);
        row.setVector(0, new float[]{1.0f, 2.0f});
        byte[] bytes = row.toBytes();
        ByteBuffer view = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int slotOffset = BinaryRow.calculateNullBitsSizeInBytes(1);
        long slot = view.getLong(slotOffset);
        int offset = (int) (slot >>> 32);
        view.putLong(slotOffset, ((long) offset << 32) | 5L);

        BinaryRow corrupted = BinaryRow.fromBytes(1, bytes);

        assertThrows(IllegalStateException.class, () -> corrupted.getVector(0));
    }

    @Test
    public void writerResetClearsMutableBuffer() {
        BinaryRowWriter writer = new BinaryRowWriter(2);

        writer.writeLong(0, 1L);
        writer.writeString(1, "one");
        BinaryRow first = writer.complete();
        assertFalse(first.isNullAt(0));
        assertFalse(first.isNullAt(1));

        writer.reset();
        BinaryRow reset = writer.complete();
        assertTrue(reset.isNullAt(0));
        assertTrue(reset.isNullAt(1));
        assertEquals(reset.getFixedSizeInBytes(), reset.getSizeInBytes());
    }

    @Test
    public void writerCompleteReturnsStableSnapshot() {
        BinaryRowWriter writer = new BinaryRowWriter(2);

        writer.writeLong(0, 1L);
        writer.writeString(1, "one");
        BinaryRow completed = writer.complete();

        writer.reset();
        writer.writeLong(0, 2L);
        writer.writeString(1, "two");
        BinaryRow next = writer.complete();

        assertEquals(1L, completed.getLong(0));
        assertEquals("one", completed.getString(1));
        assertEquals(2L, next.getLong(0));
        assertEquals("two", next.getString(1));
    }

    @Test
    public void rejectsInvalidOrdinals() {
        BinaryRow row = new BinaryRow(1);

        assertThrows(IndexOutOfBoundsException.class, () -> row.setLong(-1, 1L));
        assertThrows(IndexOutOfBoundsException.class, () -> row.getLong(1));
    }

    private String repeat(String value, int count) {
        StringBuilder builder = new StringBuilder(value.length() * count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }
}
