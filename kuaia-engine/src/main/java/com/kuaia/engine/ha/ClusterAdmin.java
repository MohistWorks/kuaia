package com.kuaia.engine.ha;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Operator-side membership changes for a running coordinator cluster, speaking the Raft admin API
 * directly (no coordinator gRPC involved). Adding a node is the second half of the standard two-step
 * join: the new coordinator starts first (peers including itself, idle outside the quorum), then
 * {@link #addNode} commits the configuration that pulls it in. Removing the current leader first
 * transfers leadership to a surviving member so the removal is safe from any starting point.
 */
public final class ClusterAdmin implements AutoCloseable {

    /** One cluster member as reported by {@link #info()}. */
    public record Member(String id, String address, String role) {
    }

    /** Cluster snapshot: elected leader (null while none is visible) and all members with roles. */
    public record ClusterView(String leaderId, List<Member> members) {
    }

    public static final String DEFAULT_GROUP_NAME = RaftPeers.DEFAULT_GROUP_NAME;
    private static final long TRANSFER_TIMEOUT_MILLIS = 10_000L;

    private final List<RaftPeer> peers;
    private final RaftGroupId groupId;
    private final RaftClient client;

    public ClusterAdmin(String peersSpec, String groupName) {
        this.peers = RaftPeers.parse(peersSpec);
        this.groupId = RaftPeers.groupId(groupName);
        RaftProperties props = new RaftProperties();
        RaftClientConfigKeys.Rpc.setRequestTimeout(props, TimeDuration.valueOf(3, TimeUnit.SECONDS));
        this.client = RaftClient.newBuilder()
                .setProperties(props)
                .setRaftGroup(RaftGroup.valueOf(groupId, peers))
                .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                        20, TimeDuration.valueOf(500, TimeUnit.MILLISECONDS)))
                .build();
    }

    /** Commit a configuration containing the current peers plus {@code newPeerSpec} ({@code id@host:port}). */
    public List<RaftPeer> addNode(String newPeerSpec) throws IOException {
        RaftPeer added = RaftPeers.parseOne(newPeerSpec);
        for (RaftPeer peer : peers) {
            if (peer.getId().equals(added.getId())) {
                throw new IllegalArgumentException(
                        "Node id '" + added.getId() + "' is already a cluster member");
            }
        }
        List<RaftPeer> next = new ArrayList<>(peers);
        next.add(added);
        RaftClientReply reply = client.admin().setConfiguration(next);
        if (!reply.isSuccess()) {
            throw new IOException("setConfiguration failed: " + reply.getException());
        }
        return next;
    }

    /**
     * Commit a configuration without {@code nodeId}. If the target currently leads the group,
     * leadership is transferred to another member first so the removal is always safe.
     */
    public List<RaftPeer> removeNode(String nodeId) throws IOException {
        List<RaftPeer> next = new ArrayList<>();
        RaftPeer target = null;
        for (RaftPeer peer : peers) {
            if (peer.getId().toString().equals(nodeId)) {
                target = peer;
            } else {
                next.add(peer);
            }
        }
        if (target == null) {
            throw new IllegalArgumentException("Node id '" + nodeId + "' is not in the peer list");
        }
        if (next.isEmpty()) {
            throw new IllegalArgumentException("Refusing to remove the last cluster node");
        }
        if (nodeId.equals(info().leaderId())) {
            client.admin().transferLeadership(next.get(0).getId(), TRANSFER_TIMEOUT_MILLIS);
        }
        RaftClientReply reply = client.admin().setConfiguration(next);
        if (!reply.isSuccess()) {
            throw new IOException("setConfiguration failed: " + reply.getException());
        }
        return next;
    }

    /** Poll every peer for its role; unreachable peers are reported, not fatal. */
    public ClusterView info() {
        String leaderId = null;
        List<Member> members = new ArrayList<>();
        for (RaftPeer peer : peers) {
            String role;
            try {
                GroupInfoReply reply = client.getGroupManagementApi(
                        RaftPeerId.valueOf(peer.getId().toString())).info(groupId);
                role = reply.getRoleInfoProto().getRole().name();
            } catch (Exception e) {
                role = "UNREACHABLE";
            }
            if ("LEADER".equals(role)) {
                leaderId = peer.getId().toString();
            }
            members.add(new Member(peer.getId().toString(), peer.getAddress(), role));
        }
        return new ClusterView(leaderId, members);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
