package com.kuaia.common.data;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BinaryRow {
    static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
    // Allocation-free views over the backing byte[]. These replace the per-call
    // ByteBuffer.wrap allocation on the per-field hot path (every slot read/write,
    // and every string/vector offset slot, routes through these). The JDK enforces
    // BYTE_ORDER, so the byte layout is identical to the previous ByteBuffer path.
    private static final VarHandle LONG_HANDLE =
            MethodHandles.byteArrayViewVarHandle(long[].class, BYTE_ORDER);
    private static final VarHandle FLOAT_HANDLE =
            MethodHandles.byteArrayViewVarHandle(float[].class, BYTE_ORDER);
    private static final VarHandle DOUBLE_HANDLE =
            MethodHandles.byteArrayViewVarHandle(double[].class, BYTE_ORDER);
    // Reserved for internal row flags such as row kind. It is not a public CDC contract.
    static final int RESERVED_HEADER_BITS = 8;
    static final int SLOT_SIZE_IN_BYTES = Long.BYTES;
    private static final int DEFAULT_VARIABLE_CAPACITY = 1024;

    private byte[] buffer;
    private final int fieldCount;
    private final int nullBitsSizeInBytes;
    private final int fixedSizeInBytes;
    private int sizeInBytes;

    public BinaryRow(int fieldCount) {
        this(fieldCount, DEFAULT_VARIABLE_CAPACITY);
    }

    BinaryRow(int fieldCount, int initialVariableCapacity) {
        if (fieldCount < 0) {
            throw new IllegalArgumentException("fieldCount must be non-negative");
        }
        if (initialVariableCapacity < 0) {
            throw new IllegalArgumentException("initialVariableCapacity must be non-negative");
        }
        this.fieldCount = fieldCount;
        this.nullBitsSizeInBytes = calculateNullBitsSizeInBytes(fieldCount);
        this.fixedSizeInBytes = calculateFixedSizeInBytes(fieldCount);
        this.buffer = new byte[fixedSizeInBytes + initialVariableCapacity];
        this.sizeInBytes = fixedSizeInBytes;
        setAllNull();
    }

    private BinaryRow(int fieldCount, byte[] bytes) {
        if (fieldCount < 0) {
            throw new IllegalArgumentException("fieldCount must be non-negative");
        }
        this.fieldCount = fieldCount;
        this.nullBitsSizeInBytes = calculateNullBitsSizeInBytes(fieldCount);
        this.fixedSizeInBytes = calculateFixedSizeInBytes(fieldCount);
        if (bytes.length < fixedSizeInBytes) {
            throw new IllegalArgumentException(
                    "Row payload is smaller than fixed section: " + bytes.length + " < " + fixedSizeInBytes);
        }
        this.buffer = bytes.clone();
        this.sizeInBytes = bytes.length;
    }

    public static int calculateNullBitsSizeInBytes(int fieldCount) {
        if (fieldCount < 0) {
            throw new IllegalArgumentException("fieldCount must be non-negative");
        }
        return ((fieldCount + 63 + RESERVED_HEADER_BITS) / 64) * Long.BYTES;
    }

    public static int calculateFixedSizeInBytes(int fieldCount) {
        return calculateNullBitsSizeInBytes(fieldCount) + fieldCount * SLOT_SIZE_IN_BYTES;
    }

    public static BinaryRow fromBytes(int fieldCount, byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes must not be null");
        }
        return new BinaryRow(fieldCount, bytes);
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public int getFixedSizeInBytes() {
        return fixedSizeInBytes;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    public boolean isNullAt(int ordinal) {
        checkOrdinal(ordinal);
        int bitIndex = RESERVED_HEADER_BITS + ordinal;
        int byteIndex = bitIndex >>> 3;
        int bitInByte = bitIndex & 7;
        return (buffer[byteIndex] & (1 << bitInByte)) != 0;
    }

    public void setNullAt(int ordinal) {
        checkOrdinal(ordinal);
        setNullBit(ordinal);
        putLong(fieldOffset(ordinal), 0L);
    }

    public void setBoolean(int ordinal, boolean value) {
        setLongSlot(ordinal, value ? 1L : 0L);
    }

    public boolean getBoolean(int ordinal) {
        return getLongSlot(ordinal) != 0L;
    }

    public void setInt(int ordinal, int value) {
        setLongSlot(ordinal, value);
    }

    public int getInt(int ordinal) {
        return (int) getLongSlot(ordinal);
    }

    public void setLong(int ordinal, long value) {
        setLongSlot(ordinal, value);
    }

    public long getLong(int ordinal) {
        return getLongSlot(ordinal);
    }

    public void setFloat(int ordinal, float value) {
        checkOrdinal(ordinal);
        clearNullBit(ordinal);
        putLong(fieldOffset(ordinal), 0L);
        FLOAT_HANDLE.set(buffer, fieldOffset(ordinal), value);
    }

    public float getFloat(int ordinal) {
        checkOrdinal(ordinal);
        checkNotNull(ordinal);
        return (float) FLOAT_HANDLE.get(buffer, fieldOffset(ordinal));
    }

    public void setDouble(int ordinal, double value) {
        checkOrdinal(ordinal);
        clearNullBit(ordinal);
        DOUBLE_HANDLE.set(buffer, fieldOffset(ordinal), value);
    }

    public double getDouble(int ordinal) {
        checkOrdinal(ordinal);
        checkNotNull(ordinal);
        return (double) DOUBLE_HANDLE.get(buffer, fieldOffset(ordinal));
    }

    public void setString(int ordinal, String value) {
        if (value == null) {
            setNullAt(ordinal);
            return;
        }
        writeVariableBytes(ordinal, value.getBytes(StandardCharsets.UTF_8));
    }

    public String getString(int ordinal) {
        byte[] bytes = getBytes(ordinal);
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    public void setBytes(int ordinal, byte[] value) {
        if (value == null) {
            setNullAt(ordinal);
            return;
        }
        writeVariableBytes(ordinal, value);
    }

    public byte[] getBytes(int ordinal) {
        checkOrdinal(ordinal);
        if (isNullAt(ordinal)) {
            return null;
        }
        OffsetAndSize offsetAndSize = readOffsetAndSize(ordinal);
        return Arrays.copyOfRange(buffer, offsetAndSize.offset, offsetAndSize.offset + offsetAndSize.size);
    }

    public void setVector(int ordinal, float[] values) {
        if (values == null) {
            setNullAt(ordinal);
            return;
        }
        int bytesLength = values.length * Float.BYTES;
        byte[] bytes = new byte[bytesLength];
        for (int i = 0; i < values.length; i++) {
            FLOAT_HANDLE.set(bytes, i * Float.BYTES, values[i]);
        }
        writeVariableBytes(ordinal, bytes);
    }

    public float[] getVector(int ordinal) {
        byte[] bytes = getBytes(ordinal);
        if (bytes == null) {
            return null;
        }
        if (bytes.length % Float.BYTES != 0) {
            throw new IllegalStateException("Invalid vector payload length: " + bytes.length);
        }
        int elementCount = bytes.length / Float.BYTES;
        float[] result = new float[elementCount];
        for (int i = 0; i < elementCount; i++) {
            result[i] = (float) FLOAT_HANDLE.get(bytes, i * Float.BYTES);
        }
        return result;
    }

    public BinaryRow copy() {
        return fromBytes(fieldCount, toBytes());
    }

    public byte[] toBytes() {
        return Arrays.copyOf(buffer, sizeInBytes);
    }

    void resetToNulls() {
        Arrays.fill(buffer, 0, fixedSizeInBytes, (byte) 0);
        sizeInBytes = fixedSizeInBytes;
        setAllNull();
    }

    private void setAllNull() {
        for (int i = 0; i < fieldCount; i++) {
            setNullBit(i);
        }
    }

    private void setLongSlot(int ordinal, long value) {
        checkOrdinal(ordinal);
        clearNullBit(ordinal);
        putLong(fieldOffset(ordinal), value);
    }

    private long getLongSlot(int ordinal) {
        checkOrdinal(ordinal);
        checkNotNull(ordinal);
        return readLong(fieldOffset(ordinal));
    }

    private void writeVariableBytes(int ordinal, byte[] bytes) {
        checkOrdinal(ordinal);
        int offset = append(bytes);
        putOffsetAndSize(ordinal, offset, bytes.length);
        clearNullBit(ordinal);
    }

    private int append(byte[] bytes) {
        int offset = sizeInBytes;
        ensureCapacity(sizeInBytes + bytes.length);
        System.arraycopy(bytes, 0, buffer, offset, bytes.length);
        sizeInBytes += bytes.length;
        return offset;
    }

    private void putOffsetAndSize(int ordinal, int offset, int size) {
        putLong(fieldOffset(ordinal), ((long) offset << 32) | (size & 0xFFFFFFFFL));
    }

    private OffsetAndSize readOffsetAndSize(int ordinal) {
        long encoded = readLong(fieldOffset(ordinal));
        int offset = (int) (encoded >>> 32);
        int size = (int) encoded;
        long end = (long) offset + size;
        if (offset < fixedSizeInBytes || size < 0 || end > sizeInBytes) {
            throw new IllegalStateException("Invalid variable field offset or size");
        }
        return new OffsetAndSize(offset, size);
    }

    private void setNullBit(int ordinal) {
        int bitIndex = RESERVED_HEADER_BITS + ordinal;
        buffer[bitIndex >>> 3] = (byte) (buffer[bitIndex >>> 3] | (1 << (bitIndex & 7)));
    }

    private void clearNullBit(int ordinal) {
        int bitIndex = RESERVED_HEADER_BITS + ordinal;
        buffer[bitIndex >>> 3] = (byte) (buffer[bitIndex >>> 3] & ~(1 << (bitIndex & 7)));
    }

    private void checkNotNull(int ordinal) {
        if (isNullAt(ordinal)) {
            throw new IllegalStateException("Field ordinal " + ordinal + " is null");
        }
    }

    private int fieldOffset(int ordinal) {
        return nullBitsSizeInBytes + ordinal * SLOT_SIZE_IN_BYTES;
    }

    private void checkOrdinal(int ordinal) {
        if (ordinal < 0 || ordinal >= fieldCount) {
            throw new IndexOutOfBoundsException(
                    "Field ordinal " + ordinal + " is out of bounds for field count " + fieldCount);
        }
    }

    private void ensureCapacity(int required) {
        if (required <= buffer.length) {
            return;
        }
        int newLength = buffer.length == 0 ? required : buffer.length;
        while (newLength < required) {
            newLength += newLength >> 1;
            if (newLength < required) {
                newLength = Math.max(newLength, required);
            }
        }
        buffer = Arrays.copyOf(buffer, newLength);
    }

    private void putLong(int offset, long value) {
        LONG_HANDLE.set(buffer, offset, value);
    }

    private long readLong(int offset) {
        return (long) LONG_HANDLE.get(buffer, offset);
    }

    private static final class OffsetAndSize {
        private final int offset;
        private final int size;

        private OffsetAndSize(int offset, int size) {
            this.offset = offset;
            this.size = size;
        }
    }
}
