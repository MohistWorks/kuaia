package com.kuaia.common.model;

import lombok.Data;
import java.io.Serializable;
import java.util.Map;

@Data
public class TaskDefinition implements Serializable {
    private String taskId;
    private String jobName;
    private Map<String, Object> config;
}
