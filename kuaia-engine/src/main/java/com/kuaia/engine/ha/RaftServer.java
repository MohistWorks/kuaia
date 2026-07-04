package com.kuaia.engine.ha;

import com.kuaia.engine.coordinator.state.RocksDBStateMachine;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Bootstraps one Raft node (Ratis) backed by a {@link RocksDBStateMachine}. Peers are given as
 * explicit {@code id@host:port} entries; this node is identified by matching {@code id}. The
 * {@link RaftGroupId} is derived from a stable group name so every node in a cluster joins the same
 * group. Election timeouts are tunable (production defaults; tests pass short values).
 */
public class RaftServer {
    private static final TimeDuration DEFAULT_TIMEOUT_MIN = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    private static final TimeDuration DEFAULT_TIMEOUT_MAX = TimeDuration.valueOf(2, TimeUnit.SECONDS);
    private static final String DEFAULT_GROUP_NAME = RaftPeers.DEFAULT_GROUP_NAME;

    private org.apache.ratis.server.RaftServer server;
    private RocksDBStateMachine stateMachine;
    private RaftGroupId groupId;
    private RaftGroup group;

    /** Start with production defaults (1s/2s election timeout, "kuaia" group). */
    public void start(String id, String peers, File storageDir) throws Exception {
        start(id, peers, storageDir, DEFAULT_TIMEOUT_MIN, DEFAULT_TIMEOUT_MAX, DEFAULT_GROUP_NAME);
    }

    /**
     * @param id         this node's id; must match one of the {@code peers} entries
     * @param peers      comma-separated {@code id@host:port} entries for the whole cluster
     * @param storageDir Raft + RocksDB storage directory
     * @param timeoutMin election timeout lower bound
     * @param timeoutMax election timeout upper bound
     * @param groupName  stable cluster name; the RaftGroupId is derived from it
     */
    public void start(String id, String peers, File storageDir,
                      TimeDuration timeoutMin, TimeDuration timeoutMax, String groupName) throws Exception {
        RaftProperties properties = new RaftProperties();
        if (!storageDir.exists()) {
            storageDir.mkdirs();
        }
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        List<RaftPeer> raftPeers = RaftPeers.parse(peers);
        String selfAddress = null;
        for (RaftPeer peer : raftPeers) {
            if (peer.getId().toString().equals(id)) {
                selfAddress = peer.getAddress();
            }
        }
        if (selfAddress == null) {
            throw new IllegalArgumentException("Node id '" + id + "' not found in peers: " + peers);
        }

        configureGrpcAddress(properties, selfAddress);
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties, timeoutMin);
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties, timeoutMax);

        this.groupId = RaftPeers.groupId(groupName);
        this.group = RaftGroup.valueOf(groupId, raftPeers);

        stateMachine = new RocksDBStateMachine();
        stateMachine.initialize(new File(storageDir, "rocksdb").getAbsolutePath());

        server = org.apache.ratis.server.RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf(id))
                .setGroup(group)
                .setProperties(properties)
                .setStateMachine(stateMachine)
                .build();
        server.start();
    }

    static void configureGrpcAddress(RaftProperties properties, String address) {
        int separator = address.lastIndexOf(':');
        if (separator <= 0 || separator == address.length() - 1) {
            throw new IllegalArgumentException("Expected advertised address in host:port form: " + address);
        }
        String host = address.substring(0, separator);
        int port = Integer.parseInt(address.substring(separator + 1));
        GrpcConfigKeys.Server.setHost(properties, host);
        GrpcConfigKeys.Server.setPort(properties, port);
    }

    /** True when this node currently leads its Raft group; false while starting or not leader. */
    public boolean isLeader() {
        if (server == null || groupId == null) {
            return false;
        }
        try {
            return server.getDivision(groupId).getInfo().isLeader();
        } catch (Exception e) {
            return false;
        }
    }

    public RaftGroup getGroup() {
        return group;
    }

    public RaftGroupId getGroupId() {
        return groupId;
    }

    public void close() throws Exception {
        if (server != null) {
            server.close();
        }
        if (stateMachine != null) {
            stateMachine.close();
        }
    }

    public org.apache.ratis.server.RaftServer getInternalServer() {
        return server;
    }
}
