package com.kuaia.engine.ha;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Shared parsing for the cluster's peer notation. Peers are written {@code id@host:port} and lists
 * are comma-separated; the {@link RaftGroupId} derives from a stable group name so every node (and
 * every admin client) addressing the same name lands in the same group.
 */
public final class RaftPeers {

    /** Default cluster group name shared by servers and admin clients. */
    public static final String DEFAULT_GROUP_NAME = "kuaia";

    private RaftPeers() {
    }

    /** Parse a comma-separated {@code id@host:port} list. */
    public static List<RaftPeer> parse(String peers) {
        List<RaftPeer> parsed = new ArrayList<>();
        for (String entry : peers.split(",")) {
            parsed.add(parseOne(entry));
        }
        return parsed;
    }

    /** Parse one {@code id@host:port} entry. */
    public static RaftPeer parseOne(String entry) {
        String e = entry.trim();
        int at = e.indexOf('@');
        if (at <= 0 || at == e.length() - 1) {
            throw new IllegalArgumentException("Expected peer in id@host:port form: " + e);
        }
        String address = e.substring(at + 1);
        int colon = address.lastIndexOf(':');
        if (colon <= 0 || colon == address.length() - 1) {
            throw new IllegalArgumentException("Expected peer in id@host:port form: " + e);
        }
        try {
            Integer.parseInt(address.substring(colon + 1));
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Expected peer in id@host:port form: " + e);
        }
        return RaftPeer.newBuilder().setId(e.substring(0, at)).setAddress(address).build();
    }

    /** Stable group id for a cluster name; equal names always yield the same id. */
    public static RaftGroupId groupId(String groupName) {
        return RaftGroupId.valueOf(UUID.nameUUIDFromBytes(groupName.getBytes(StandardCharsets.UTF_8)));
    }
}
