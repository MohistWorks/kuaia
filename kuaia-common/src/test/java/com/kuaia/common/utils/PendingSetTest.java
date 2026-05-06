package com.kuaia.common.utils;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class PendingSetTest {
    @Test
    public void testContinuousPrefix() {
        PendingSet set = new PendingSet();
        set.add(1);
        set.add(2);
        set.add(4);
        assertEquals(2, set.getHighestContinuous(0));
    }
}
