package com.kuaia.common.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class TaskBundle implements Serializable {
    private String taskId;
    private String jobId;
    private List<Object> splits;
}
