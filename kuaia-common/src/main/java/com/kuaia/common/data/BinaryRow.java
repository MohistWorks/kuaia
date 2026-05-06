package com.kuaia.common.data;

public class BinaryRow {
    private byte[] buffer;
    
    public BinaryRow(int numFields) {
        // Initial simplified layout: [NullBitmap: 8 bytes] + [FixedSection: numFields * 8 bytes]
        this.buffer = new byte[8 + numFields * 8];
    }
    
    public void setLong(int ordinal, long value) {
        int pos = 8 + ordinal * 8;
        for (int i = 0; i < 8; i++) {
            buffer[pos + i] = (byte) (value >> (i * 8));
        }
    }
    
    public long getLong(int ordinal) {
        int pos = 8 + ordinal * 8;
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (buffer[pos + i] & 0xFF)) << (i * 8);
        }
        return value;
    }
}
