package com.kuaia.common.data;

public class BinaryRowWriter {
    private final BinaryRow row;

    public BinaryRowWriter(int fieldCount) {
        this(new BinaryRow(fieldCount));
    }

    public BinaryRowWriter(BinaryRow row) {
        if (row == null) {
            throw new IllegalArgumentException("row must not be null");
        }
        this.row = row;
    }

    public void reset() {
        row.resetToNulls();
    }

    public void setNullAt(int ordinal) {
        row.setNullAt(ordinal);
    }

    public void writeBoolean(int ordinal, boolean value) {
        row.setBoolean(ordinal, value);
    }

    public void writeInt(int ordinal, int value) {
        row.setInt(ordinal, value);
    }

    public void writeLong(int ordinal, long value) {
        row.setLong(ordinal, value);
    }

    public void writeFloat(int ordinal, float value) {
        row.setFloat(ordinal, value);
    }

    public void writeDouble(int ordinal, double value) {
        row.setDouble(ordinal, value);
    }

    public void writeString(int ordinal, String value) {
        row.setString(ordinal, value);
    }

    public void writeBytes(int ordinal, byte[] value) {
        row.setBytes(ordinal, value);
    }

    public void writeVector(int ordinal, float[] value) {
        row.setVector(ordinal, value);
    }

    public BinaryRow complete() {
        return row.copy();
    }
}
