package com.kuaia.engine.ha;

import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftPeersTest {

    @Test
    void parsesCommaSeparatedPeerList() {
        List<RaftPeer> peers = RaftPeers.parse("n1@127.0.0.1:6001, n2@127.0.0.1:6002,n3@10.0.0.3:6003");

        assertEquals(3, peers.size());
        assertEquals("n1", peers.get(0).getId().toString());
        assertEquals("127.0.0.1:6001", peers.get(0).getAddress());
        assertEquals("n2", peers.get(1).getId().toString());
        assertEquals("10.0.0.3:6003", peers.get(2).getAddress());
    }

    @Test
    void rejectsMalformedEntries() {
        assertThrows(IllegalArgumentException.class, () -> RaftPeers.parseOne("no-at-sign:6001"));
        assertThrows(IllegalArgumentException.class, () -> RaftPeers.parseOne("@127.0.0.1:6001"));
        assertThrows(IllegalArgumentException.class, () -> RaftPeers.parseOne("n1@hostonly"));
        assertThrows(IllegalArgumentException.class, () -> RaftPeers.parseOne("n1@127.0.0.1:"));
        assertThrows(IllegalArgumentException.class, () -> RaftPeers.parseOne("n1@127.0.0.1:abc"));
    }

    @Test
    void groupIdIsStableForTheSameName() {
        assertEquals(RaftPeers.groupId("kuaia"), RaftPeers.groupId("kuaia"));
        assertNotEquals(RaftPeers.groupId("kuaia"), RaftPeers.groupId("other"));
    }
}
