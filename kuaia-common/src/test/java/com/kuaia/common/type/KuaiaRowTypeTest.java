package com.kuaia.common.type;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KuaiaRowTypeTest {
    @Test
    public void legacyConstructorDefaultsToNullableFields() {
        KuaiaRowType rowType = new KuaiaRowType(
                new String[]{"id", "content"},
                new DataType[]{DataType.LONG, DataType.STRING});

        assertEquals(2, rowType.getFieldCount());
        assertEquals(0, rowType.getIndex("id"));
        assertEquals(1, rowType.getIndex("content"));
        assertEquals(-1, rowType.getIndex("missing"));
        assertTrue(rowType.isNullable(0));
        assertTrue(rowType.isNullable(1));
        assertArrayEquals(new String[]{"id", "content"}, rowType.getFieldNames());
        assertArrayEquals(new DataType[]{DataType.LONG, DataType.STRING}, rowType.getFieldTypes());
    }

    @Test
    public void fieldConstructorKeepsMetadata() {
        KuaiaRowType rowType = new KuaiaRowType(new KuaiaRowType.Field[]{
                new KuaiaRowType.Field("id", DataType.LONG, false, "stable id"),
                new KuaiaRowType.Field("payload", DataType.BYTES, true, "raw bytes")
        });

        assertEquals(2, rowType.getFieldCount());
        assertEquals("id", rowType.getField(0).getName());
        assertEquals(DataType.LONG, rowType.getField(0).getType());
        assertFalse(rowType.getField(0).isNullable());
        assertEquals("stable id", rowType.getField(0).getDescription());
        assertEquals(1, rowType.getIndex("payload"));
    }

    @Test
    public void keepsDefensiveCopies() {
        String[] names = new String[]{"id"};
        DataType[] types = new DataType[]{DataType.LONG};
        boolean[] nullable = new boolean[]{false};

        KuaiaRowType rowType = new KuaiaRowType(names, types, nullable);
        names[0] = "changed";
        types[0] = DataType.STRING;
        nullable[0] = true;

        assertEquals("id", rowType.getFieldName(0));
        assertEquals(DataType.LONG, rowType.getFieldType(0));
        assertFalse(rowType.isNullable(0));

        String[] returnedNames = rowType.getFieldNames();
        DataType[] returnedTypes = rowType.getFieldTypes();
        boolean[] returnedNullability = rowType.getNullability();
        returnedNames[0] = "other";
        returnedTypes[0] = DataType.BOOLEAN;
        returnedNullability[0] = true;

        assertEquals("id", rowType.getFieldName(0));
        assertEquals(DataType.LONG, rowType.getFieldType(0));
        assertFalse(rowType.isNullable(0));
    }

    @Test
    public void rejectsBrokenSchemas() {
        assertThrows(IllegalArgumentException.class, () -> new KuaiaRowType(
                new String[]{"id"},
                new DataType[]{DataType.LONG, DataType.STRING}));
        assertThrows(IllegalArgumentException.class, () -> new KuaiaRowType(
                new String[]{"id", "id"},
                new DataType[]{DataType.LONG, DataType.STRING}));
        assertThrows(IllegalArgumentException.class, () -> new KuaiaRowType(
                new String[]{" "},
                new DataType[]{DataType.LONG}));
    }
}
