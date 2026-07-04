package com.kuaia.engine.ha;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Local-validation behavior of {@link ClusterAdmin}: every rejection here must throw before any
 * network I/O, so these tests run against fake addresses with no cluster.
 */
class ClusterAdminTest {

    private static final String PEERS = "n1@127.0.0.1:6001,n2@127.0.0.1:6002,n3@127.0.0.1:6003";

    @Test
    void addNodeRejectsDuplicateId() throws Exception {
        try (ClusterAdmin admin = new ClusterAdmin(PEERS, ClusterAdmin.DEFAULT_GROUP_NAME)) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> admin.addNode("n2@127.0.0.1:6004"));
            assertTrue(e.getMessage().contains("already a cluster member"));
        }
    }

    @Test
    void addNodeRejectsMalformedPeer() throws Exception {
        try (ClusterAdmin admin = new ClusterAdmin(PEERS, ClusterAdmin.DEFAULT_GROUP_NAME)) {
            assertThrows(IllegalArgumentException.class, () -> admin.addNode("n4-no-address"));
        }
    }

    @Test
    void removeNodeRejectsUnknownId() throws Exception {
        try (ClusterAdmin admin = new ClusterAdmin(PEERS, ClusterAdmin.DEFAULT_GROUP_NAME)) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> admin.removeNode("nope"));
            assertTrue(e.getMessage().contains("not in the peer list"));
        }
    }

    @Test
    void removeNodeRefusesLastMember() throws Exception {
        try (ClusterAdmin admin = new ClusterAdmin("n1@127.0.0.1:6001", ClusterAdmin.DEFAULT_GROUP_NAME)) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> admin.removeNode("n1"));
            assertTrue(e.getMessage().contains("last cluster node"));
        }
    }
}
