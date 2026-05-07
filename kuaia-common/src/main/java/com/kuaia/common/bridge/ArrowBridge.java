package com.kuaia.common.bridge;

import com.kuaia.common.data.BinaryRow;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VarCharVector;
import java.nio.charset.StandardCharsets;
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

    public static void stringsToArrow(List<BinaryRow> rows, int ordinal, VarCharVector vector) {
        vector.allocateNew(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            byte[] bytes = rows.get(i).getString(ordinal).getBytes(StandardCharsets.UTF_8);
            vector.setSafe(i, bytes);
        }
        vector.setValueCount(rows.size());
    }

    public static void writeVectorsToRows(Float4Vector arrowVector, List<BinaryRow> rows, int ordinal, int dim) {
        for (int i = 0; i < rows.size(); i++) {
            float[] vector = new float[dim];
            for (int d = 0; d < dim; d++) {
                // Assuming the Arrow vector contains elements for all rows sequentially
                vector[d] = arrowVector.get(i * dim + d);
            }
            rows.get(i).setVector(ordinal, vector);
        }
    }
}
