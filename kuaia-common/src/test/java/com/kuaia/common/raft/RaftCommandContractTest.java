package com.kuaia.common.raft;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftCommandContractTest {
    @Test
    void raftCommandCarriesTaskRecordPayload() {
        RaftCommand command = RaftCommand.newBuilder()
                .setType(CommandType.SAVE_TASK_RECORD)
                .setTaskRecord(TaskRecordPayload.newBuilder()
                        .setTaskId("task-1")
                        .setRecord(ByteString.copyFromUtf8("record-bytes"))
                        .build())
                .build();

        assertTrue(command.hasTaskRecord());
        assertEquals(CommandType.SAVE_TASK_RECORD, command.getType());
        assertEquals("task-1", command.getTaskRecord().getTaskId());
    }
}
