package com.kuaia.common.model;

import java.io.Serializable;

public class WorkerRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum WorkerState {
        REGISTERED,
        ONLINE,
        PAUSED,
        OFFLINE
    }

    public enum BackpressureLevel {
        LOW,
        HIGH
    }

    private final String workerId;
    private final String host;
    private final int port;
    private final WorkerState state;
    private final double loadScore;
    private final int activeTaskCount;
    private final long lastHeartbeatMillis;
    private final boolean streamConnected;
    private final BackpressureLevel backpressureLevel;

    public WorkerRecord(
            String workerId,
            String host,
            int port,
            WorkerState state,
            double loadScore,
            int activeTaskCount,
            long lastHeartbeatMillis,
            boolean streamConnected,
            BackpressureLevel backpressureLevel) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;
        this.state = state;
        this.loadScore = loadScore;
        this.activeTaskCount = activeTaskCount;
        this.lastHeartbeatMillis = lastHeartbeatMillis;
        this.streamConnected = streamConnected;
        this.backpressureLevel = backpressureLevel;
    }

    public static WorkerRecord registered(String workerId, String host, int port) {
        return new WorkerRecord(
                workerId,
                host,
                port,
                WorkerState.REGISTERED,
                0.0,
                0,
                System.currentTimeMillis(),
                false,
                BackpressureLevel.LOW);
    }

    public WorkerRecord withState(WorkerState state) {
        return new WorkerRecord(
                workerId,
                host,
                port,
                state,
                loadScore,
                activeTaskCount,
                lastHeartbeatMillis,
                streamConnected,
                backpressureLevel);
    }

    public String getWorkerId() {
        return workerId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public WorkerState getState() {
        return state;
    }

    public double getLoadScore() {
        return loadScore;
    }

    public int getActiveTaskCount() {
        return activeTaskCount;
    }

    public long getLastHeartbeatMillis() {
        return lastHeartbeatMillis;
    }

    public boolean isStreamConnected() {
        return streamConnected;
    }

    public BackpressureLevel getBackpressureLevel() {
        return backpressureLevel;
    }
}
