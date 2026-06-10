package com.kuaia.common.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class JobInstance implements Serializable {
    private static final long serialVersionUID = 1L;

    private String jobId;
    private TaskState state = TaskState.CREATED;
    private List<String> taskIds;

    // Per-job terminal-task counters maintained incrementally so the job's aggregate state can be
    // recomputed in O(1) per task transition instead of re-scanning every child. The total task
    // count is derived from taskIds, so it is not stored here.
    private int completedTasks;
    private int failedTasks;
    private int cancelledTasks;
}
