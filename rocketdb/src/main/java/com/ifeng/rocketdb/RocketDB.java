package com.ifeng.rocketdb;

import org.rocksdb.*;


import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * RocksDB的包装类
 * 增加了日志的记录
 * TODO:字节数组使用缓冲池
 * Created by lijs
 * on 2017/7/31.
 */
public class RocketDB {
    private Logger logger;

    private RocketDB() {

    }

    private static RocketDB rocketDB = new RocketDB();

    public static RocketDB getInstance() {
        return rocketDB;
    }

    private volatile boolean init = false;


    //TODO:换其他的线程池？
    private ThreadPoolExecutor workers;

    ThreadPoolExecutor getWorkers() {
        return workers;
    }


    private RocksDB db;
    private Cache cache;
    private ColumnFamilyOptions cfOpts;
    private DBOptions options;
    private List<ColumnFamilyHandle> columnFamilyHandleList;

    synchronized void init(Map<Object, Object> config) {
        if (init) {
            return;
        }
        init = true;
        db = initRocksDB(config);

        long cacheSize = Long.parseLong((String) config.get("cache_size")) * 1024L * 1024L;
        cache = new LRUCache(cacheSize);
        workers = new ThreadPoolExecutor(8, 16, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1024));
    }

    //TODO:init
    private RocksDB initRocksDB(Map<Object, Object> config) {
        //初始化
        cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();

        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor("my-first-columnfamily".getBytes(), cfOpts)
        );

        options = new DBOptions();
        options.setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        options.setDbLogDir((String) config.get("logDir"));
        options.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);

        columnFamilyHandleList = new ArrayList<>();
        try {
            db = RocksDB.open(options, (String) config.get("dbpath"), cfDescriptors, columnFamilyHandleList);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        return db;
    }

    public void set(byte[] key, byte[] value) throws RocksDBException {
        db.put(key, value);

    }

    public byte[] get(byte[] key) throws RocksDBException {
        return db.get(key);
    }

    public void get(byte[] key, byte[] value) throws RocksDBException {
        db.get(key, value);
    }

    public void del(byte[] key) throws RocksDBException {
        db.delete(key);
    }

    void shutdown() {
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
            columnFamilyHandle.close();
        }
        db.close();
        options.close();
        cfOpts.close();
        cache.close();
        logger.close();
    }

}
