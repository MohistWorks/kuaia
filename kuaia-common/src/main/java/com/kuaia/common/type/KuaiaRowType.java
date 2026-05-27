package com.kuaia.common.type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class KuaiaRowType implements Serializable {
    private final Field[] fields;
    private final Map<String, Integer> indexByName;

    public KuaiaRowType(String[] fieldNames, DataType[] fieldTypes) {
        this(fieldNames, fieldTypes, nullableDefaults(fieldNames));
    }

    public KuaiaRowType(String[] fieldNames, DataType[] fieldTypes, boolean[] nullable) {
        validateLegacyInputs(fieldNames, fieldTypes, nullable);
        Field[] copiedFields = new Field[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            copiedFields[i] = new Field(fieldNames[i], fieldTypes[i], nullable[i]);
        }
        this.fields = copiedFields;
        this.indexByName = buildIndex(copiedFields);
    }

    public KuaiaRowType(Field[] fields) {
        Objects.requireNonNull(fields, "fields must not be null");
        this.fields = new Field[fields.length];
        for (int i = 0; i < fields.length; i++) {
            this.fields[i] = Objects.requireNonNull(fields[i], "field must not be null");
        }
        this.indexByName = buildIndex(this.fields);
    }

    public int getFieldCount() {
        return fields.length;
    }

    public Field getField(int ordinal) {
        checkOrdinal(ordinal);
        return fields[ordinal];
    }

    public String getFieldName(int ordinal) {
        return getField(ordinal).getName();
    }

    public DataType getFieldType(int ordinal) {
        return getField(ordinal).getType();
    }

    public boolean isNullable(int ordinal) {
        return getField(ordinal).isNullable();
    }

    public Field[] getFields() {
        return fields.clone();
    }

    public String[] getFieldNames() {
        String[] names = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            names[i] = fields[i].getName();
        }
        return names;
    }

    public DataType[] getFieldTypes() {
        DataType[] types = new DataType[fields.length];
        for (int i = 0; i < fields.length; i++) {
            types[i] = fields[i].getType();
        }
        return types;
    }

    public boolean[] getNullability() {
        boolean[] nullable = new boolean[fields.length];
        for (int i = 0; i < fields.length; i++) {
            nullable[i] = fields[i].isNullable();
        }
        return nullable;
    }

    public int getIndex(String fieldName) {
        Integer index = indexByName.get(fieldName);
        return index == null ? -1 : index;
    }

    private static boolean[] nullableDefaults(String[] fieldNames) {
        Objects.requireNonNull(fieldNames, "fieldNames must not be null");
        boolean[] nullable = new boolean[fieldNames.length];
        Arrays.fill(nullable, true);
        return nullable;
    }

    private static void validateLegacyInputs(String[] fieldNames, DataType[] fieldTypes, boolean[] nullable) {
        Objects.requireNonNull(fieldNames, "fieldNames must not be null");
        Objects.requireNonNull(fieldTypes, "fieldTypes must not be null");
        Objects.requireNonNull(nullable, "nullable must not be null");
        if (fieldNames.length != fieldTypes.length || fieldNames.length != nullable.length) {
            throw new IllegalArgumentException("Field names, types, and nullability must have the same length");
        }
    }

    private static Map<String, Integer> buildIndex(Field[] fields) {
        Set<String> seen = new HashSet<>();
        Map<String, Integer> index = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (field.getName().trim().isEmpty()) {
                throw new IllegalArgumentException("Field name must not be blank");
            }
            if (!seen.add(field.getName())) {
                throw new IllegalArgumentException("Duplicate field name: " + field.getName());
            }
            index.put(field.getName(), i);
        }
        return Map.copyOf(index);
    }

    private void checkOrdinal(int ordinal) {
        if (ordinal < 0 || ordinal >= fields.length) {
            throw new IndexOutOfBoundsException(
                    "Field ordinal " + ordinal + " is out of bounds for field count " + fields.length);
        }
    }

    public static final class Field implements Serializable {
        private final String name;
        private final DataType type;
        private final boolean nullable;
        private final String description;

        public Field(String name, DataType type, boolean nullable) {
            this(name, type, nullable, null);
        }

        public Field(String name, DataType type, boolean nullable, String description) {
            this.name = Objects.requireNonNull(name, "name must not be null");
            this.type = Objects.requireNonNull(type, "type must not be null");
            this.nullable = nullable;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public DataType getType() {
            return type;
        }

        public boolean isNullable() {
            return nullable;
        }

        public String getDescription() {
            return description;
        }
    }
}
