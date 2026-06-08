package com.kuaia.common.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class JobInstance implements Serializable {
    private String jobId;
    private TaskState state = TaskState.CREATED;
    private List<String> taskIds;
}
