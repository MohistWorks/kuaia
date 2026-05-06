package com.kuaia.common.bridge;

import com.kuaia.common.data.BinaryRow;
import org.apache.arrow.vector.BigIntVector;
import java.util.List;

public class ArrowBridge {
    public static void convertToArrow(List<BinaryRow> rows, BigIntVector vector) {
        vector.allocateNew(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            // Convert the first field (long) of each BinaryRow to BigIntVector
            vector.set(i, rows.get(i).getLong(0));
        }
        vector.setValueCount(rows.size());
    }
}
