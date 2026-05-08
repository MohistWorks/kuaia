package com.kuaia.engine.worker.transform;

import com.kuaia.common.data.BinaryRow;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class EmbeddingTransform {
    private final int textOrdinal;
    private final int vectorOrdinal;
    private final int batchSize;
    private final List<BinaryRow> buffer = new ArrayList<>();

    public EmbeddingTransform() {
        this(1, 2, 5);
    }

    public EmbeddingTransform(int textOrdinal, int vectorOrdinal, int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.textOrdinal = textOrdinal;
        this.vectorOrdinal = vectorOrdinal;
        this.batchSize = batchSize;
    }

    public void process(BinaryRow row, Consumer<List<BinaryRow>> output) {
        buffer.add(row);
        if (buffer.size() >= batchSize) {
            applyEmbedding(buffer);
            output.accept(new ArrayList<>(buffer));
            buffer.clear();
        }
    }

    private void applyEmbedding(List<BinaryRow> batch) {
        // Simulated AI Embedding logic.
        // In a real scenario, this would use ArrowBridge to batch-call an LLM.
        for (BinaryRow row : batch) {
            String text = row.getString(textOrdinal);
            float[] mockVector = new float[4]; // Dimension 4 for mock
            for (int i = 0; i < 4; i++) {
                mockVector[i] = text.length() + i;
            }
            row.setVector(vectorOrdinal, mockVector);
        }
    }
}
