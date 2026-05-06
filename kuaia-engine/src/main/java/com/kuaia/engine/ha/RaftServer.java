package com.kuaia.engine.ha;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import java.io.File;
import java.util.Collections;

public class RaftServer {
    private org.apache.ratis.server.RaftServer server;

    public void start(String id, String address, String peerAddress, File storageDir) throws Exception {
        RaftPeer peer = RaftPeer.newBuilder().setId(id).setAddress(address).build();
        RaftProperties properties = new RaftProperties();
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
        
        server = org.apache.ratis.server.RaftServer.newBuilder()
                .setServerId(peer.getId())
                .setGroup(RaftGroup.valueOf(RaftGroupId.randomId(), peer))
                .setProperties(properties)
                .setStateMachine(new BaseStateMachine())
                .build();
        server.start();
    }
    
    public void close() throws Exception {
        if (server != null) {
            server.close();
        }
    }
}
