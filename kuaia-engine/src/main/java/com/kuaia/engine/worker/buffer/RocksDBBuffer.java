package com.kuaia.engine.worker.buffer;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import java.io.File;

public class RocksDBBuffer {
    private RocksDB db;

    public void open(String path) throws Exception {
        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);
        db = RocksDB.open(options, path);
    }

    public void put(long seqId, byte[] data) throws Exception {
        db.put(String.valueOf(seqId).getBytes(), data);
    }

    public byte[] get(long seqId) throws Exception {
        return db.get(String.valueOf(seqId).getBytes());
    }

    public void close() {
        if (db != null) db.close();
    }
}
