import com.hyd.ssdb.SsdbClient;
import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by lijs
 * on 2017/7/31.
 */
public class TestRocksDB {
    private byte[][] keys = new byte[200 * 10000][];
    private RocksDB db;

    private void init() throws RocksDBException {
        String path = "J:\\work\\ifeng\\ssdbtester\\src\\main\\files\\uuids";
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            for (int i = 0; i < keys.length; i++) {
                keys[i] = reader.readLine().getBytes();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        RocksDB.loadLibrary();
    }

    @Test
    public void testPut() throws RocksDBException {
        init();
        Options options = new Options();
        //options.setErrorIfExists(true);
        options.setCreateIfMissing(true);

        db = RocksDB.open(options, "J:\\work\\rocksdb\\data");
        long start = System.currentTimeMillis();

        /*for (int i = 0; i < 100000; i++) {
            byte[] bytes = db.get(keys[i]);
            if (!new String(bytes).equals(String.valueOf(i))) {
                System.out.println(new String(bytes));
            }
        }*/
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(false);

        for (int i = 0; i < 100000; i++) {
            db.put(keys[i], String.valueOf(i + 100000).getBytes());
        }

        System.out.println(System.currentTimeMillis() -start);
        db.close();

    }

    @Test
    public void testGet() throws RocksDBException {
        init();
        Options options = new Options();
        //options.setErrorIfExists(true);
        options.setCreateIfMissing(true);

        db = RocksDB.open(options, "J:\\work\\rocksdb\\data");
        long start = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++) {
            byte[] bytes = db.get(keys[i]);
            if (!new String(bytes).equals(String.valueOf(i))) {
                System.out.println(new String(bytes));
            }
        }

        System.out.println(System.currentTimeMillis() -start);
        db.close();
    }

    @Test
    public void test1() throws RocksDBException {
        db = RocksDB.open(new Options(), "J:\\work\\rocksdb\\data");
        db.put("test1".getBytes(), "first".getBytes());
        db.put("test1".getBytes(), "last".getBytes());
        byte[] res = new byte[100];
        System.out.println(db.get("test1".getBytes(), res));
        System.out.println(new String(res));
        db.close();
    }

    @Test
    public void test2() {
        SsdbClient client = new SsdbClient("127.0.0.1", 10000, 1);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            client.set("test" + i, "" + i + 10000);
        }
        System.out.println(System.currentTimeMillis() - start);
        client.close();
    }

    @Test
    public void test3() {
        SsdbClient client = new SsdbClient("127.0.0.1", 10000, 5);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            String s = client.get("test" + i);
            Assert.assertEquals(s, "" + i + 10000);
        }
        System.out.println(System.currentTimeMillis() - start);
        client.close();
    }

    @Test
    public void test4() {
        SsdbClient client = new SsdbClient("127.0.0.1", 10000, 5);
        StringBuilder builder =  new StringBuilder();
        for (int i = 0; i < 200; i++) {
            builder.append(i);
        }
        client.set("builder", builder.toString());
        String s = client.get("builder");
        System.out.println(s.equals(builder.toString()));
        client.close();
    }

    @Test
    public void test5() {
        final SsdbClient client = new SsdbClient("127.0.0.1", 10000, 5);
        long start = System.currentTimeMillis();
        final Random random = new Random();
        Thread[] threads = new Thread[1];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 100; i++) {
                        client.hset(String.valueOf(UUID.randomUUID()), "hp-" + random.nextInt(6), "86");
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(System.currentTimeMillis() - start);
        client.close();
    }

    @Test
    public void test6() {
        SsdbClient client = new SsdbClient("172.30.160.67", 8888);

        String key = "longKey";

        String res = client.get(key);
        System.out.println(res);
    }

    @Test
    public void test7() {
        SsdbClient client = new SsdbClient("127.0.0.1", 9999, 100);

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            builder.append(i);
        }
        //builder.append(10000);

        String key = "longKey";
        //client.set(key, builder.toString());
        System.out.println(client.get(key).equals(builder.toString()));
    }

    @Test
    public void test8() {
        SsdbClient client = new SsdbClient("127.0.0.1", 9999);

        String key = "longKey";

        String res = client.get(key);
        System.out.println(res);
    }


    @Test
    public void testAuth() {
        SsdbClient client = new SsdbClient("10.90.13.99", 8888);

        String key = "0";


        client.sendRequest("AUTH", "11111111111111111111111111111111");
        String res = client.get(key);
        System.out.println(res);
    }

    @Test
    public void testLRUCache() throws RocksDBException {
        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();

        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor("my-first-columnfamily".getBytes(), cfOpts)
        );

        DBOptions options = new DBOptions();
        options.setCreateIfMissing(false).setCreateMissingColumnFamilies(true);

        List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

        db = RocksDB.open(options, "J:\\work\\rocksdb\\data", cfDescriptors, columnFamilyHandleList);

        System.out.println(new String(db.get("test1".getBytes())));

        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
            columnFamilyHandle.close();
        }

        db.close();
        options.close();

        cfOpts.close();

    }

    @Test
    public void testColumnFamily() throws RocksDBException {
        init();
        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();

        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor("my-first-columnfamily".getBytes(), cfOpts)
        );

        DBOptions options = new DBOptions();
        options.setCreateIfMissing(false).setCreateMissingColumnFamilies(true);

        List<ColumnFamilyHandle> columnFamilyHandleList =
                new ArrayList<>();

        db = RocksDB.open(options, "J:\\work\\rocksdb\\data", cfDescriptors, columnFamilyHandleList);

        ColumnFamilyHandle defalutFamily = columnFamilyHandleList.get(0);
        ColumnFamilyHandle myColumnfamily = columnFamilyHandleList.get(1);

        db.put(defalutFamily, "lang".getBytes(), "java".getBytes());
        db.put(myColumnfamily, "lang".getBytes(), "python".getBytes());

        System.out.println("default: " + new String(db.get("lang".getBytes())));
        System.out.println("my: " + new String(db.get(myColumnfamily, "lang".getBytes())));


        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
            columnFamilyHandle.close();
        }

        db.close();
        options.close();

        cfOpts.close();
    }

}
