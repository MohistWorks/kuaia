package com.kuaia.engine.worker.executor;

import com.kuaia.common.rpc.TaskPayload;
import java.util.concurrent.CompletableFuture;

public class TaskExecutor {
    public CompletableFuture<Boolean> execute(TaskPayload task) {
        return CompletableFuture.supplyAsync(() -> {
            // Simulated AI/LLM work
            try { Thread.sleep(100); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            return true;
        });
    }
}
