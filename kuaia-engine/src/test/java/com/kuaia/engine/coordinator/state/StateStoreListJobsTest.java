package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.JobInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateStoreListJobsTest {

    private JobInstance job(String id) {
        JobInstance j = new JobInstance();
        j.setJobId(id);
        j.setTaskIds(Arrays.asList(id + "-t0", id + "-t1"));
        return j;
    }

    @Test
    void inMemoryListsSubmittedJobs() {
        InMemoryStateStore store = new InMemoryStateStore();
        store.submitJob(job("a"));
        store.submitJob(job("b"));
        List<String> ids = store.listJobs().stream().map(JobInstance::getJobId).collect(Collectors.toList());
        assertEquals(2, ids.size());
        assertTrue(ids.contains("a") && ids.contains("b"));
    }

    @Test
    void rocksDbListsSubmittedJobs(@TempDir Path tmp) throws Exception {
        try (RocksDbStateStore store = new RocksDbStateStore(tmp.resolve("state"))) {
            store.submitJob(job("a"));
            store.submitJob(job("b"));
            List<String> ids = store.listJobs().stream().map(JobInstance::getJobId).collect(Collectors.toList());
            assertEquals(2, ids.size());
            assertTrue(ids.contains("a") && ids.contains("b"));
        }
    }
}
