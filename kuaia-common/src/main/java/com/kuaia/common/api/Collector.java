package com.kuaia.common.api;
import com.kuaia.common.data.BinaryRow;

public interface Collector {
    void collect(BinaryRow row);
}
