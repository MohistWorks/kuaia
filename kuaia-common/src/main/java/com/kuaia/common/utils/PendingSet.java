package com.kuaia.common.utils;

import org.roaringbitmap.RoaringBitmap;

public class PendingSet {
    private final RoaringBitmap bitmap = new RoaringBitmap();

    public synchronized void add(long seqId) {
        bitmap.add((int) seqId);
    }

    public synchronized void remove(long seqId) {
        bitmap.remove((int) seqId);
    }

    public synchronized long getHighestContinuous(long start) {
        long current = start;
        while (bitmap.contains((int) (current + 1))) {
            current++;
        }
        return current;
    }
}
