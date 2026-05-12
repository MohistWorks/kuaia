package com.kuaia.engine.coordinator.state;

import com.kuaia.common.model.TaskRecord;
import com.kuaia.common.model.TaskState;
import com.kuaia.common.model.WorkerRecord;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class RocksDbStateStore implements StateStore, Closeable {
    private static final byte[] EMPTY = new byte[0];
    private static final String TASK_PREFIX = "task/";
    private static final String TASK_STATE_PREFIX = "task_state/";
    private static final String TASK_WORKER_PREFIX = "task_worker/";
    private static final String WORKER_PREFIX = "worker/";
    private static final String WORKER_STATE_PREFIX = "worker_state/";

    private final Object lock = new Object();
    private final Options options;
    private final WriteOptions writeOptions;
    private final RocksDB db;

    public RocksDbStateStore(Path path) throws IOException, RocksDBException {
        RocksDB.loadLibrary();
        Files.createDirectories(path);
        this.options = new Options().setCreateIfMissing(true);
        this.writeOptions = new WriteOptions();
        this.db = RocksDB.open(options, path.toString());
    }

    @Override
    public void saveTask(TaskRecord record) {
        synchronized (lock) {
            try (WriteBatch batch = new WriteBatch()) {
                TaskRecord previous = getTaskInternal(record.getTaskId());
                deleteTaskIndexes(batch, previous);
                putTask(batch, record);
                db.write(writeOptions, batch);
            } catch (RocksDBException e) {
                throw new IllegalStateException("Failed to save task " + record.getTaskId(), e);
            }
        }
    }

    @Override
    public TaskRecord getTask(String taskId) {
        synchronized (lock) {
            try {
                return getTaskInternal(taskId);
            } catch (RocksDBException e) {
                throw new IllegalStateException("Failed to read task " + taskId, e);
            }
        }
    }

    @Override
    public boolean compareAndSetTask(TaskRecord expected, TaskRecord updated) {
        if (!expected.getTaskId().equals(updated.getTaskId())) {
            return false;
        }
        synchronized (lock) {
            try (WriteBatch batch = new WriteBatch()) {
                TaskRecord current = getTaskInternal(expected.getTaskId());
                if (current == null || current.getVersion() != expected.getVersion()) {
                    return false;
                }
                deleteTaskIndexes(batch, current);
                putTask(batch, updated);
                db.write(writeOptions, batch);
                return true;
            } catch (RocksDBException e) {
                throw new IllegalStateException("Failed to update task " + expected.getTaskId(), e);
            }
        }
    }

    @Override
    public List<TaskRecord> scanTasksByState(TaskState state) {
        synchronized (lock) {
            String prefix = taskStatePrefix(state);
            return scanIdsByPrefix(prefix).stream()
                    .map(this::getTask)
                    .filter(record -> record != null)
                    .sorted(Comparator.comparing(TaskRecord::getTaskId))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public List<TaskRecord> scanActiveTasksByWorker(String workerId) {
        synchronized (lock) {
            String prefix = taskWorkerPrefix(workerId);
            return scanIdsByPrefix(prefix).stream()
                    .map(this::getTask)
                    .filter(record -> record != null)
                    .sorted(Comparator.comparing(TaskRecord::getTaskId))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void saveWorker(WorkerRecord record) {
        synchronized (lock) {
            try (WriteBatch batch = new WriteBatch()) {
                WorkerRecord previous = getWorkerInternal(record.getWorkerId());
                deleteWorkerIndexes(batch, previous);
                putWorker(batch, record);
                db.write(writeOptions, batch);
            } catch (RocksDBException e) {
                throw new IllegalStateException("Failed to save worker " + record.getWorkerId(), e);
            }
        }
    }

    @Override
    public WorkerRecord getWorker(String workerId) {
        synchronized (lock) {
            try {
                return getWorkerInternal(workerId);
            } catch (RocksDBException e) {
                throw new IllegalStateException("Failed to read worker " + workerId, e);
            }
        }
    }

    @Override
    public List<WorkerRecord> scanWorkersByState(WorkerRecord.WorkerState state) {
        synchronized (lock) {
            String prefix = workerStatePrefix(state);
            return scanIdsByPrefix(prefix).stream()
                    .map(this::getWorker)
                    .filter(record -> record != null)
                    .sorted(Comparator.comparing(WorkerRecord::getWorkerId))
                    .collect(Collectors.toList());
        }
    }

    private TaskRecord getTaskInternal(String taskId) throws RocksDBException {
        return deserialize(db.get(bytes(taskKey(taskId))), TaskRecord.class);
    }

    private WorkerRecord getWorkerInternal(String workerId) throws RocksDBException {
        return deserialize(db.get(bytes(workerKey(workerId))), WorkerRecord.class);
    }

    private void putTask(WriteBatch batch, TaskRecord record) throws RocksDBException {
        batch.put(bytes(taskKey(record.getTaskId())), serialize(record));
        batch.put(bytes(taskStateKey(record)), EMPTY);
        if (isActive(record.getState()) && record.getAssignedWorkerId() != null) {
            batch.put(bytes(taskWorkerKey(record.getAssignedWorkerId(), record.getTaskId())), EMPTY);
        }
    }

    private void deleteTaskIndexes(WriteBatch batch, TaskRecord record) throws RocksDBException {
        if (record == null) {
            return;
        }
        batch.delete(bytes(taskStateKey(record)));
        if (isActive(record.getState()) && record.getAssignedWorkerId() != null) {
            batch.delete(bytes(taskWorkerKey(record.getAssignedWorkerId(), record.getTaskId())));
        }
    }

    private void putWorker(WriteBatch batch, WorkerRecord record) throws RocksDBException {
        batch.put(bytes(workerKey(record.getWorkerId())), serialize(record));
        batch.put(bytes(workerStateKey(record)), EMPTY);
    }

    private void deleteWorkerIndexes(WriteBatch batch, WorkerRecord record) throws RocksDBException {
        if (record != null) {
            batch.delete(bytes(workerStateKey(record)));
        }
    }

    private List<String> scanIdsByPrefix(String prefix) {
        List<String> ids = new ArrayList<>();
        byte[] prefixBytes = bytes(prefix);
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seek(prefixBytes); iterator.isValid(); iterator.next()) {
                String key = string(iterator.key());
                if (!key.startsWith(prefix)) {
                    break;
                }
                ids.add(key.substring(prefix.length()));
            }
            iterator.status();
            return ids;
        } catch (RocksDBException e) {
            throw new IllegalStateException("Failed to scan RocksDB prefix " + prefix, e);
        }
    }

    private boolean isActive(TaskState state) {
        return state == TaskState.DISPATCHING || state == TaskState.RUNNING;
    }

    private String taskKey(String taskId) {
        return TASK_PREFIX + taskId;
    }

    private String taskStateKey(TaskRecord record) {
        return taskStatePrefix(record.getState()) + record.getTaskId();
    }

    private String taskStatePrefix(TaskState state) {
        return TASK_STATE_PREFIX + state.name() + "/";
    }

    private String taskWorkerKey(String workerId, String taskId) {
        return taskWorkerPrefix(workerId) + taskId;
    }

    private String taskWorkerPrefix(String workerId) {
        return TASK_WORKER_PREFIX + workerId + "/";
    }

    private String workerKey(String workerId) {
        return WORKER_PREFIX + workerId;
    }

    private String workerStateKey(WorkerRecord record) {
        return workerStatePrefix(record.getState()) + record.getWorkerId();
    }

    private String workerStatePrefix(WorkerRecord.WorkerState state) {
        return WORKER_STATE_PREFIX + state.name() + "/";
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private String string(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private byte[] serialize(Object value) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
            objectStream.writeObject(value);
            objectStream.flush();
            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize " + value.getClass().getSimpleName(), e);
        }
    }

    private <T> T deserialize(byte[] bytes, Class<T> type) {
        if (bytes == null) {
            return null;
        }
        try (ObjectInputStream objectStream = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return type.cast(objectStream.readObject());
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Failed to deserialize " + type.getSimpleName(), e);
        }
    }

    @Override
    public void close() {
        db.close();
        writeOptions.close();
        options.close();
    }
}
