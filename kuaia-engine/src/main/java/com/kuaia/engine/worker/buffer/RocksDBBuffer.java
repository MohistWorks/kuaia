package com.kuaia.engine.worker.buffer;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import java.io.File;

public class RocksDBBuffer {
    private RocksDB db;
    private Options options;

    public void open(String path) throws Exception {
        RocksDB.loadLibrary();
        Options newOptions = new Options().setCreateIfMissing(true);
        try {
            db = RocksDB.open(newOptions, path);
            options = newOptions;
        } catch (Exception e) {
            newOptions.close();
            throw e;
        }
    }

    public void put(long seqId, byte[] data) throws Exception {
        db.put(String.valueOf(seqId).getBytes(), data);
    }

    public byte[] get(long seqId) throws Exception {
        return db.get(String.valueOf(seqId).getBytes());
    }

    public void close() {
        RuntimeException failure = null;
        if (db != null) {
            try {
                db.close();
            } catch (RuntimeException e) {
                failure = e;
            } finally {
                db = null;
            }
        }
        if (options != null) {
            try {
                options.close();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                options = null;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    boolean isOptionsOwningHandleForTesting() {
        return options != null && options.isOwningHandle();
    }
}
