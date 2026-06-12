package com.kuaia.engine.worker;

import com.kuaia.common.model.TaskDefinition;
import com.kuaia.common.rpc.AttemptStatus;
import com.kuaia.common.rpc.CoordinatorMessage;
import com.kuaia.common.rpc.CoordinatorServiceGrpc;
import com.kuaia.common.rpc.TaskAssignment;
import com.kuaia.common.rpc.WorkerMessage;
import com.google.protobuf.ByteString;
import com.kuaia.engine.coordinator.planner.JobSubmissionService;
import com.kuaia.engine.pipeline.ConnectorFactory;
import com.kuaia.engine.pipeline.PipelineConfig;
import com.kuaia.engine.worker.connector.SinkFactoryRegistry;
import com.kuaia.engine.worker.connector.v2.SourceEnumerator;
import com.kuaia.engine.worker.connector.v2.SourceSplit;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerNodeTest {
    @TempDir
    Path tmp;

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

    @Test
    void workerCompletesTypedTaskAssignment() throws Exception {
        // Build a real file -> identity -> file pipeline so the worker actually executes it
        // through the gRPC stream wiring and reports ATTEMPT_SUCCESS on completion.
        Path input = tmp.resolve("input.csv");
        Files.write(input, String.join("\n",
                "id",
                "1",
                "2",
                "3").getBytes(StandardCharsets.UTF_8));
        Path output = tmp.resolve("output.csv");

        PipelineConfig cfg = new PipelineConfig(
                "file-to-file",
                new PipelineConfig.SourceConfig("file", input.toString(), "csv"),
                new PipelineConfig.SinkConfig("file", output.toString(), "csv", "overwrite"),
                new PipelineConfig.CheckpointConfig(null));

        ConnectorFactory factory = new ConnectorFactory(SinkFactoryRegistry.defaultRegistry());
        List<SourceSplit> splits;
        SourceEnumerator src = factory.createSource(cfg);
        src.open();
        try {
            splits = src.enumerateSplits();
        } finally {
            src.close();
        }
        long firstSeq = splits.get(0).getStartSeqInclusive();
        ByteString definition = serializedPipelineDefinition("task-1", cfg, splits);

        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<WorkerMessage> resultMessage = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0)
                .addService(new CoordinatorServiceGrpc.CoordinatorServiceImplBase() {
                    @Override
                    public StreamObserver<WorkerMessage> taskStream(
                            StreamObserver<CoordinatorMessage> responseObserver) {
                        return new StreamObserver<WorkerMessage>() {
                            @Override
                            public void onNext(WorkerMessage value) {
                                if (value.hasHello()) {
                                    responseObserver.onNext(CoordinatorMessage.newBuilder()
                                            .setAssignment(TaskAssignment.newBuilder()
                                                    .setTaskId("task-1")
                                                    .setAttemptId("attempt-1")
                                                    .setDefinition(definition)
                                                    .setStartSeq(firstSeq)
                                                    .setLeaseUntilMillis(System.currentTimeMillis() + 10_000L)
                                                    .build())
                                            .build());
                                }
                                if (value.hasTaskResult()) {
                                    resultMessage.compareAndSet(null, value);
                                    completed.countDown();
                                }
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
        String workerId = "worker-assignment-test-" + System.nanoTime();
        WorkerNode worker = new WorkerNode(workerId);

        try {
            worker.start("127.0.0.1", server.getPort());

            assertTrue(completed.await(5, TimeUnit.SECONDS), "worker should complete typed assignment");
            WorkerMessage message = resultMessage.get();
            assertEquals(workerId, message.getWorkerId());
            assertTrue(message.hasTaskResult());
            assertEquals("task-1", message.getTaskResult().getTaskId());
            assertEquals("attempt-1", message.getTaskResult().getAttemptId());
            assertEquals(workerId, message.getTaskResult().getWorkerId());
            assertEquals(AttemptStatus.ATTEMPT_SUCCESS, message.getTaskResult().getStatus());

            assertTrue(Files.exists(output), "output file should exist");
            List<String> lines = Files.readAllLines(output, StandardCharsets.UTF_8);
            // CSV output = header line + one line per data row (3 rows).
            assertEquals(4, lines.size());
        } finally {
            worker.stop();
            server.shutdownNow();
            server.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void workerRejectsExpiredTypedTaskAssignmentLease() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<WorkerMessage> resultMessage = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0)
                .addService(new CoordinatorServiceGrpc.CoordinatorServiceImplBase() {
                    @Override
                    public StreamObserver<WorkerMessage> taskStream(
                            StreamObserver<CoordinatorMessage> responseObserver) {
                        return new StreamObserver<WorkerMessage>() {
                            @Override
                            public void onNext(WorkerMessage value) {
                                if (value.hasHello()) {
                                    responseObserver.onNext(CoordinatorMessage.newBuilder()
                                            .setAssignment(TaskAssignment.newBuilder()
                                                    .setTaskId("task-1")
                                                    .setAttemptId("attempt-1")
                                                    .setDefinition(serializedDefinition("task-1"))
                                                    .setStartSeq(1L)
                                                    .setLeaseUntilMillis(System.currentTimeMillis() - 1_000L)
                                                    .build())
                                            .build());
                                }
                                if (value.hasTaskResult()) {
                                    resultMessage.compareAndSet(null, value);
                                    completed.countDown();
                                }
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
        String workerId = "worker-expired-assignment-test-" + System.nanoTime();
        WorkerNode worker = new WorkerNode(workerId);

        try {
            worker.start("127.0.0.1", server.getPort());

            assertTrue(completed.await(3, TimeUnit.SECONDS), "worker should reject expired assignment lease");
            WorkerMessage message = resultMessage.get();
            assertEquals(workerId, message.getWorkerId());
            assertTrue(message.hasTaskResult());
            assertEquals("task-1", message.getTaskResult().getTaskId());
            assertEquals("attempt-1", message.getTaskResult().getAttemptId());
            assertEquals(workerId, message.getTaskResult().getWorkerId());
            assertEquals(AttemptStatus.ATTEMPT_FAILED, message.getTaskResult().getStatus());
            assertEquals("INVALID_ASSIGNMENT", message.getTaskResult().getErrorCode());
        } finally {
            worker.stop();
            server.shutdownNow();
            server.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void workerRejectsMalformedTypedTaskAssignment() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<WorkerMessage> resultMessage = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0)
                .addService(new CoordinatorServiceGrpc.CoordinatorServiceImplBase() {
                    @Override
                    public StreamObserver<WorkerMessage> taskStream(
                            StreamObserver<CoordinatorMessage> responseObserver) {
                        return new StreamObserver<WorkerMessage>() {
                            @Override
                            public void onNext(WorkerMessage value) {
                                if (value.hasHello()) {
                                    responseObserver.onNext(CoordinatorMessage.newBuilder()
                                            .setAssignment(TaskAssignment.newBuilder()
                                                    .setTaskId("task-1")
                                                    .setStartSeq(1L)
                                                    .setLeaseUntilMillis(System.currentTimeMillis() + 10_000L)
                                                    .build())
                                            .build());
                                }
                                if (value.hasTaskResult()) {
                                    resultMessage.compareAndSet(null, value);
                                    completed.countDown();
                                }
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
        String workerId = "worker-malformed-assignment-test-" + System.nanoTime();
        WorkerNode worker = new WorkerNode(workerId);

        try {
            worker.start("127.0.0.1", server.getPort());

            assertTrue(completed.await(3, TimeUnit.SECONDS), "worker should reject malformed assignment");
            WorkerMessage message = resultMessage.get();
            assertEquals(workerId, message.getWorkerId());
            assertTrue(message.hasTaskResult());
            assertEquals("task-1", message.getTaskResult().getTaskId());
            assertEquals(workerId, message.getTaskResult().getWorkerId());
            assertEquals(AttemptStatus.ATTEMPT_FAILED, message.getTaskResult().getStatus());
            assertEquals("INVALID_ASSIGNMENT", message.getTaskResult().getErrorCode());
        } finally {
            worker.stop();
            server.shutdownNow();
            server.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void workerRejectsTypedTaskAssignmentWithoutDefinition() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<WorkerMessage> resultMessage = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0)
                .addService(new CoordinatorServiceGrpc.CoordinatorServiceImplBase() {
                    @Override
                    public StreamObserver<WorkerMessage> taskStream(
                            StreamObserver<CoordinatorMessage> responseObserver) {
                        return new StreamObserver<WorkerMessage>() {
                            @Override
                            public void onNext(WorkerMessage value) {
                                if (value.hasHello()) {
                                    responseObserver.onNext(CoordinatorMessage.newBuilder()
                                            .setAssignment(TaskAssignment.newBuilder()
                                                    .setTaskId("task-1")
                                                    .setAttemptId("attempt-1")
                                                    .setStartSeq(1L)
                                                    .setLeaseUntilMillis(System.currentTimeMillis() + 10_000L)
                                                    .build())
                                            .build());
                                }
                                if (value.hasTaskResult()) {
                                    resultMessage.compareAndSet(null, value);
                                    completed.countDown();
                                }
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
        String workerId = "worker-missing-definition-test-" + System.nanoTime();
        WorkerNode worker = new WorkerNode(workerId);

        try {
            worker.start("127.0.0.1", server.getPort());

            assertTrue(completed.await(3, TimeUnit.SECONDS), "worker should reject assignment without definition");
            WorkerMessage message = resultMessage.get();
            assertEquals(workerId, message.getWorkerId());
            assertTrue(message.hasTaskResult());
            assertEquals("task-1", message.getTaskResult().getTaskId());
            assertEquals("attempt-1", message.getTaskResult().getAttemptId());
            assertEquals(workerId, message.getTaskResult().getWorkerId());
            assertEquals(AttemptStatus.ATTEMPT_FAILED, message.getTaskResult().getStatus());
            assertEquals("INVALID_ASSIGNMENT", message.getTaskResult().getErrorCode());
        } finally {
            worker.stop();
            server.shutdownNow();
            server.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void workerRejectsTypedTaskAssignmentWithInvalidDefinitionBytes() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<WorkerMessage> resultMessage = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0)
                .addService(new CoordinatorServiceGrpc.CoordinatorServiceImplBase() {
                    @Override
                    public StreamObserver<WorkerMessage> taskStream(
                            StreamObserver<CoordinatorMessage> responseObserver) {
                        return new StreamObserver<WorkerMessage>() {
                            @Override
                            public void onNext(WorkerMessage value) {
                                if (value.hasHello()) {
                                    responseObserver.onNext(CoordinatorMessage.newBuilder()
                                            .setAssignment(TaskAssignment.newBuilder()
                                                    .setTaskId("task-1")
                                                    .setAttemptId("attempt-1")
                                                    .setDefinition(ByteString.copyFromUtf8("not-a-serialized-task-definition"))
                                                    .setStartSeq(1L)
                                                    .setLeaseUntilMillis(System.currentTimeMillis() + 10_000L)
                                                    .build())
                                            .build());
                                }
                                if (value.hasTaskResult()) {
                                    resultMessage.compareAndSet(null, value);
                                    completed.countDown();
                                }
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
        String workerId = "worker-invalid-definition-test-" + System.nanoTime();
        WorkerNode worker = new WorkerNode(workerId);

        try {
            worker.start("127.0.0.1", server.getPort());

            assertTrue(completed.await(3, TimeUnit.SECONDS), "worker should reject invalid definition bytes");
            WorkerMessage message = resultMessage.get();
            assertEquals(workerId, message.getWorkerId());
            assertTrue(message.hasTaskResult());
            assertEquals("task-1", message.getTaskResult().getTaskId());
            assertEquals("attempt-1", message.getTaskResult().getAttemptId());
            assertEquals(workerId, message.getTaskResult().getWorkerId());
            assertEquals(AttemptStatus.ATTEMPT_FAILED, message.getTaskResult().getStatus());
            assertEquals("INVALID_ASSIGNMENT", message.getTaskResult().getErrorCode());
        } finally {
            worker.stop();
            server.shutdownNow();
            server.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    @Test
    void workerRejectsTaskAssignmentWhenDefinitionTaskIdDiffers() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<WorkerMessage> resultMessage = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0)
                .addService(new CoordinatorServiceGrpc.CoordinatorServiceImplBase() {
                    @Override
                    public StreamObserver<WorkerMessage> taskStream(
                            StreamObserver<CoordinatorMessage> responseObserver) {
                        return new StreamObserver<WorkerMessage>() {
                            @Override
                            public void onNext(WorkerMessage value) {
                                if (value.hasHello()) {
                                    responseObserver.onNext(CoordinatorMessage.newBuilder()
                                            .setAssignment(TaskAssignment.newBuilder()
                                                    .setTaskId("task-1")
                                                    .setAttemptId("attempt-1")
                                                    .setDefinition(serializedDefinition("other-task"))
                                                    .setStartSeq(1L)
                                                    .setLeaseUntilMillis(System.currentTimeMillis() + 10_000L)
                                                    .build())
                                            .build());
                                }
                                if (value.hasTaskResult()) {
                                    resultMessage.compareAndSet(null, value);
                                    completed.countDown();
                                }
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
        String workerId = "worker-mismatched-definition-test-" + System.nanoTime();
        WorkerNode worker = new WorkerNode(workerId);

        try {
            worker.start("127.0.0.1", server.getPort());

            assertTrue(completed.await(3, TimeUnit.SECONDS), "worker should reject mismatched definition task id");
            WorkerMessage message = resultMessage.get();
            assertEquals(workerId, message.getWorkerId());
            assertTrue(message.hasTaskResult());
            assertEquals("task-1", message.getTaskResult().getTaskId());
            assertEquals("attempt-1", message.getTaskResult().getAttemptId());
            assertEquals(workerId, message.getTaskResult().getWorkerId());
            assertEquals(AttemptStatus.ATTEMPT_FAILED, message.getTaskResult().getStatus());
            assertEquals("INVALID_ASSIGNMENT", message.getTaskResult().getErrorCode());
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

    private ByteString serializedDefinition(String taskId) {
        TaskDefinition definition = new TaskDefinition();
        definition.setTaskId(taskId);
        definition.setJobName("job-1");
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream objectStream = new ObjectOutputStream(bytes)) {
            objectStream.writeObject(definition);
            return ByteString.copyFrom(bytes.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize test definition", e);
        }
    }

    private ByteString serializedPipelineDefinition(
            String taskId, PipelineConfig cfg, List<SourceSplit> splits) {
        TaskDefinition definition = new TaskDefinition();
        definition.setTaskId(taskId);
        definition.setJobName("job-1");
        Map<String, Object> config = new HashMap<>();
        config.put(JobSubmissionService.SPLITS_CONFIG_KEY, new ArrayList<Object>(splits));
        config.put(WorkerTaskExecutor.PIPELINE_CONFIG_KEY, cfg);
        definition.setConfig(config);
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream objectStream = new ObjectOutputStream(bytes)) {
            objectStream.writeObject(definition);
            return ByteString.copyFrom(bytes.toByteArray());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize test definition", e);
        }
    }
}
