package com.kuaia.common.model;

import lombok.Builder;
import lombok.Data;
import java.io.Serializable;

@Data
@Builder
public class NodeInfo implements Serializable {
    private String id;
    private String host;
    private int port;
    private NodeType type;

    public enum NodeType {
        COORDINATOR, WORKER
    }
}
