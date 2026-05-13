package com.kuaia.engine.worker;

import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.common.rpc.CoordinatorServiceGrpc;
import com.kuaia.common.rpc.WorkerMessage;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerNodeTest {
    @Test
    void workerNodeDoesNotWriteDirectlyToConsole() throws Exception {
        String source = new String(
                Files.readAllBytes(repoRoot().resolve("kuaia-engine/src/main/java/com/kuaia/engine/worker/WorkerNode.java")),
                StandardCharsets.UTF_8);

        assertFalse(source.contains("System.out.println"), source);
        assertFalse(source.contains("System.err.println"), source);
        assertFalse(source.contains("printStackTrace()"), source);
    }

    @Test
    void startSendsWorkerHelloOnTaskStream() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<WorkerMessage> firstMessage = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0)
                .addService(new CoordinatorServiceGrpc.CoordinatorServiceImplBase() {
                    @Override
                    public StreamObserver<WorkerMessage> taskStream(
                            StreamObserver<CoordinatorMessage> responseObserver) {
                        return new StreamObserver<WorkerMessage>() {
                            @Override
                            public void onNext(WorkerMessage value) {
                                firstMessage.compareAndSet(null, value);
                                received.countDown();
                            }

                            @Override
                            public void onError(Throwable t) {
                            }

                            @Override
                            public void onCompleted() {
                            }
                        };
                    }
                })
                .build()
                .start();
        String workerId = "worker-hello-test-" + System.nanoTime();
        WorkerNode worker = new WorkerNode(workerId);

        try {
            worker.start("127.0.0.1", server.getPort());

            assertTrue(received.await(3, TimeUnit.SECONDS), "worker should send hello after opening task stream");
            WorkerMessage message = firstMessage.get();
            assertEquals(workerId, message.getWorkerId());
            assertTrue(message.hasHello());
            assertEquals(workerId, message.getHello().getWorkerId());
        } finally {
            worker.stop();
            server.shutdownNow();
            server.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    private Path repoRoot() {
        Path cwd = Paths.get("").toAbsolutePath();
        if (Files.exists(cwd.resolve("pom.xml")) && Files.exists(cwd.resolve("kuaia-engine"))) {
            return cwd;
        }
        return cwd.getParent();
    }
}
