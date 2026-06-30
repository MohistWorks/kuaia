package com.kuaia.engine;

import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.common.rpc.CoordinatorServiceGrpc;
import com.kuaia.common.rpc.WorkerMessage;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerRunnerTest {

    @Test
    void startConnectsAndAwaitReturnsAfterClose() throws Exception {
        CountDownLatch hello = new CountDownLatch(1);
        Server server = ServerBuilder.forPort(0)
                .addService(new CoordinatorServiceGrpc.CoordinatorServiceImplBase() {
                    @Override
                    public StreamObserver<WorkerMessage> taskStream(StreamObserver<CoordinatorMessage> r) {
                        return new StreamObserver<>() {
                            @Override public void onNext(WorkerMessage v) { if (v.hasHello()) hello.countDown(); }
                            @Override public void onError(Throwable t) { }
                            @Override public void onCompleted() { }
                        };
                    }
                })
                .build()
                .start();

        WorkerRunner runner = new WorkerRunner("worker-runner-test-" + System.nanoTime());
        try {
            runner.start("127.0.0.1", server.getPort());
            assertTrue(hello.await(3, TimeUnit.SECONDS), "worker should send hello after connecting");

            Thread awaiter = new Thread(() -> {
                try {
                    runner.awaitTermination();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });
            awaiter.start();
            Thread.sleep(100);
            assertTrue(awaiter.isAlive(), "awaitTermination should block until close()");

            runner.close();
            awaiter.join(3000);
            assertFalse(awaiter.isAlive(), "awaitTermination should return after close()");
        } finally {
            runner.close();
            server.shutdownNow();
            server.awaitTermination(3, TimeUnit.SECONDS);
        }
    }
}
