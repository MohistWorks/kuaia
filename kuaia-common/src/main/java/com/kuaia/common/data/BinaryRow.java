package com.kuaia.common.data;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BinaryRow {
    private byte[] buffer;
    private int numFields;
    private int cursor; // Pointer for variable section

    public BinaryRow(int numFields) {
        this.numFields = numFields;
        // Pre-allocate buffer: 8 (NullMap) + numFields * 8 (Slots) + 1024 (Initial Var section)
        this.buffer = new byte[8 + numFields * 8 + 1024];
        this.cursor = 8 + numFields * 8;
    }

    public void setLong(int ordinal, long value) {
        int pos = 8 + ordinal * 8;
        ByteBuffer.wrap(buffer).putLong(pos, value);
    }

    public long getLong(int ordinal) {
        int pos = 8 + ordinal * 8;
        return ByteBuffer.wrap(buffer).getLong(pos);
    }

    public void setString(int ordinal, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        int offset = cursor;
        
        System.arraycopy(bytes, 0, buffer, offset, length);
        cursor += length;
        
        // Encode offset and length into the 8-byte slot
        long encoded = ((long) offset << 32) | (long) length;
        setLong(ordinal, encoded);
    }

    public String getString(int ordinal) {
        long encoded = getLong(ordinal);
        int offset = (int) (encoded >> 32);
        int length = (int) (encoded & 0xFFFFFFFFL);
        return new String(buffer, offset, length, StandardCharsets.UTF_8);
    }
}
