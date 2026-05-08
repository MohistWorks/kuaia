package com.kuaia.engine.ha;

import com.kuaia.engine.coordinator.state.RocksDBStateMachine;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;
import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class RaftServer {
    private org.apache.ratis.server.RaftServer server;
    private RocksDBStateMachine stateMachine;

    public void start(String id, String address, String peerAddresses, File storageDir) throws Exception {
        RaftProperties properties = new RaftProperties();
        if (!storageDir.exists()) {
            storageDir.mkdirs();
        }
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
        configureGrpcAddressForTesting(properties, address);

        // Optimize for testing
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(150, TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));

        String[] addresses = peerAddresses.split(",");
        List<RaftPeer> peers = new ArrayList<>();
        for (int i = 0; i < addresses.length; i++) {
            String peerAddress = addresses[i].trim();
            String peerId = peerAddress.equals(address) ? id : "p" + (i + 1);
            peers.add(RaftPeer.newBuilder()
                    .setId(peerId)
                    .setAddress(peerAddress)
                    .build());
        }

        // Use a consistent GroupId for testing
        RaftGroupId groupId = RaftGroupId.valueOf(UUID.nameUUIDFromBytes("test-group".getBytes()));
        RaftGroup group = RaftGroup.valueOf(groupId, peers);

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

    static void configureGrpcAddressForTesting(RaftProperties properties, String address) {
        int separator = address.lastIndexOf(':');
        if (separator <= 0 || separator == address.length() - 1) {
            throw new IllegalArgumentException("Expected advertised address in host:port form: " + address);
        }
        String host = address.substring(0, separator);
        int port = Integer.parseInt(address.substring(separator + 1));
        GrpcConfigKeys.Server.setHost(properties, host);
        GrpcConfigKeys.Server.setPort(properties, port);
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
