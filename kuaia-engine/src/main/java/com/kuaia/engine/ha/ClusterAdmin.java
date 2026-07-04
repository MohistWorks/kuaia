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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(ClusterAdmin.class);

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

    /**
     * Commit a configuration containing the LIVE cluster membership plus {@code newPeerSpec}
     * ({@code id@host:port}). Membership math is based on the configuration read from the cluster,
     * never on the operator-supplied peer list alone — a stale {@code --raft-peers} must not
     * silently evict members it fails to mention.
     */
    public List<RaftPeer> addNode(String newPeerSpec) throws IOException {
        RaftPeer added = RaftPeers.parseOne(newPeerSpec);
        for (RaftPeer peer : peers) {
            if (peer.getId().equals(added.getId())) {
                throw new IllegalArgumentException(
                        "Node id '" + added.getId() + "' is already a cluster member");
            }
        }
        List<RaftPeer> live = liveMembers();
        for (RaftPeer peer : live) {
            if (peer.getId().equals(added.getId())) {
                throw new IllegalArgumentException(
                        "Node id '" + added.getId() + "' is already in the live cluster configuration");
            }
        }
        List<RaftPeer> next = new ArrayList<>(live);
        next.add(added);
        RaftClientReply reply = client.admin().setConfiguration(next);
        if (!reply.isSuccess()) {
            throw new IOException("setConfiguration failed: " + reply.getException());
        }
        return next;
    }

    /**
     * Commit the LIVE configuration without {@code nodeId} (see {@link #addNode} for why membership
     * math uses the live configuration). If the target currently leads the group, leadership is
     * transferred to a reachable member first so the removal is always safe; a failed transfer is
     * logged and removal proceeds — Ratis lets a leader commit a configuration removing itself and
     * then step down.
     */
    public List<RaftPeer> removeNode(String nodeId) throws IOException {
        boolean inSupplied = false;
        for (RaftPeer peer : peers) {
            if (peer.getId().toString().equals(nodeId)) {
                inSupplied = true;
            }
        }
        if (!inSupplied) {
            throw new IllegalArgumentException("Node id '" + nodeId + "' is not in the peer list");
        }
        if (peers.size() <= 1) {
            throw new IllegalArgumentException("Refusing to remove the last cluster node");
        }
        List<RaftPeer> next = new ArrayList<>();
        RaftPeer target = null;
        for (RaftPeer peer : liveMembers()) {
            if (peer.getId().toString().equals(nodeId)) {
                target = peer;
            } else {
                next.add(peer);
            }
        }
        if (target == null) {
            throw new IllegalArgumentException(
                    "Node id '" + nodeId + "' is not in the live cluster configuration");
        }
        if (next.isEmpty()) {
            throw new IllegalArgumentException("Refusing to remove the last cluster node");
        }
        ClusterView view = info();
        if (nodeId.equals(view.leaderId())) {
            RaftClientReply transfer = client.admin().transferLeadership(
                    transferTarget(view, next, nodeId), TRANSFER_TIMEOUT_MILLIS);
            if (!transfer.isSuccess()) {
                LOG.warn("Leadership transfer away from {} did not succeed ({}); proceeding — the "
                        + "leader steps down after committing its own removal", nodeId, transfer.getException());
            }
        }
        RaftClientReply reply = client.admin().setConfiguration(next);
        if (!reply.isSuccess()) {
            throw new IOException("setConfiguration failed: " + reply.getException());
        }
        return next;
    }

    /** Prefer a member the last {@link #info()} sweep saw as a reachable FOLLOWER. */
    private static RaftPeerId transferTarget(ClusterView view, List<RaftPeer> candidates, String removingId) {
        for (Member member : view.members()) {
            if (!member.id().equals(removingId) && "FOLLOWER".equals(member.role())) {
                return RaftPeerId.valueOf(member.id());
            }
        }
        return candidates.get(0).getId();
    }

    /** The cluster's current membership as committed in its own configuration log. */
    private List<RaftPeer> liveMembers() throws IOException {
        for (RaftPeer peer : peers) {
            try {
                GroupInfoReply reply = client.getGroupManagementApi(
                        RaftPeerId.valueOf(peer.getId().toString())).info(groupId);
                return new ArrayList<>(reply.getGroup().getPeers());
            } catch (Exception e) {
                LOG.debug("Peer {} did not answer a group info request", peer.getId(), e);
            }
        }
        throw new IOException("Cannot read the live cluster configuration from any supplied peer");
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
