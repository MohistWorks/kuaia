package com.kuaia.common.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class TaskBundle implements Serializable {
    private String taskId;
    private String jobId;
    // Source-defined opaque splits. Each element MUST be java.io.Serializable: splits ride a
    // TaskRecord's definition config through RocksDB / Raft, which serialize via ObjectOutputStream.
    private List<Object> splits;
}
