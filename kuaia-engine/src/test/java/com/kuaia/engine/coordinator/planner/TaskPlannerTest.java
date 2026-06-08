package com.kuaia.engine.coordinator.planner;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskBundle;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskPlannerTest {
    private final TaskPlanner planner = new TaskPlanner();

    @Test
    void returnsEmptyForNullOrEmptySplits() {
        JobInstance job = new JobInstance();
        job.setJobId("job-1");

        assertTrue(planner.plan(job, null, 4).isEmpty());
        assertTrue(planner.plan(job, List.of(), 4).isEmpty());
    }

    @Test
    void respectsMaxParallelism() {
        JobInstance job = new JobInstance();
        job.setJobId("job-1");
        List<Object> splits = dummySplits(100);

        List<TaskBundle> bundles = planner.plan(job, splits, 8);

        assertEquals(8, bundles.size());
        int totalSplitCount = 0;
        for (TaskBundle bundle : bundles) {
            assertEquals("job-1", bundle.getJobId());
            assertNotNull(bundle.getTaskId());
            assertTrue(bundle.getSplits().size() > 0);
            totalSplitCount += bundle.getSplits().size();
        }
        assertEquals(100, totalSplitCount);
    }

    @Test
    void fewerSplitsThanMaxParallelismCreatesOneBundlePerSplit() {
        JobInstance job = new JobInstance();
        job.setJobId("job-1");
        List<Object> splits = dummySplits(3);

        List<TaskBundle> bundles = planner.plan(job, splits, 10);

        assertEquals(3, bundles.size());
        for (TaskBundle bundle : bundles) {
            assertEquals(1, bundle.getSplits().size());
            assertEquals("job-1", bundle.getJobId());
        }
    }

    @Test
    void singleSplitCreatesSingleBundle() {
        JobInstance job = new JobInstance();
        job.setJobId("job-1");
        List<Object> splits = dummySplits(1);

        List<TaskBundle> bundles = planner.plan(job, splits, 4);

        assertEquals(1, bundles.size());
        assertEquals(1, bundles.get(0).getSplits().size());
        assertEquals("job-1", bundles.get(0).getJobId());
    }

    @Test
    void evenlyDividesSplitsWhenExactMultiple() {
        JobInstance job = new JobInstance();
        job.setJobId("job-1");
        List<Object> splits = dummySplits(12);

        List<TaskBundle> bundles = planner.plan(job, splits, 3);

        assertEquals(3, bundles.size());
        for (TaskBundle bundle : bundles) {
            assertEquals(4, bundle.getSplits().size());
        }
    }

    @Test
    void allBundleTaskIdsAreUnique() {
        JobInstance job = new JobInstance();
        job.setJobId("job-1");
        List<Object> splits = dummySplits(20);

        List<TaskBundle> bundles = planner.plan(job, splits, 5);

        long distinctIds = bundles.stream().map(TaskBundle::getTaskId).distinct().count();
        assertEquals(bundles.size(), distinctIds);
    }

    private static List<Object> dummySplits(int count) {
        List<Object> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add("split-" + UUID.randomUUID());
        }
        return splits;
    }
}
