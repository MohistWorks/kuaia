package com.kuaia.common.type;

import lombok.AllArgsConstructor;
import lombok.Getter;
import java.io.Serializable;

@AllArgsConstructor
@Getter
public class KuaiaRowType implements Serializable {
    private final String[] fieldNames;
    private final DataType[] fieldTypes;

    public int getIndex(String fieldName) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(fieldName)) return i;
        }
        return -1;
    }
}
