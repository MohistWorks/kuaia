package com.kuaia.engine.coordinator.planner;

import com.kuaia.common.model.JobInstance;
import com.kuaia.common.model.TaskBundle;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TaskPlanner {
    public List<TaskBundle> plan(JobInstance job, List<Object> sourceSplits, int maxParallelism) {
        List<TaskBundle> bundles = new ArrayList<>();
        if (sourceSplits == null || sourceSplits.isEmpty()) {
            return bundles;
        }

        int totalSplits = sourceSplits.size();
        int actualTasks = Math.min(totalSplits, maxParallelism);
        int batchSize = (int) Math.ceil((double) totalSplits / actualTasks);

        for (int i = 0; i < totalSplits; i += batchSize) {
            int end = Math.min(i + batchSize, totalSplits);
            List<Object> bundleSplits = new ArrayList<>(sourceSplits.subList(i, end));

            TaskBundle bundle = new TaskBundle();
            bundle.setTaskId(UUID.randomUUID().toString());
            bundle.setJobId(job.getJobId());
            bundle.setSplits(bundleSplits);
            bundles.add(bundle);
        }
        return bundles;
    }
}
