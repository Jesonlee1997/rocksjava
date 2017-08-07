package com.ifeng.rocketdb;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by lijs
 * on 2017/7/31.
 */
public class BytePool {

    private int level = 3;//3个层级的buffer
    private BlockingQueue<Buffer>[] buffers = new BlockingQueue[level];

    private static BytePool pool = new BytePool();
    public static BytePool getInstance() {
        return pool;
    }
    private BytePool() {
        buffers[0] = new LinkedBlockingQueue<>(10000);
        buffers[1] = new LinkedBlockingQueue<>(1000);
        buffers[2] = new LinkedBlockingQueue<>(100);
    }

    public Buffer getBuffer(int exceptLength) {
        int level = getLevel(exceptLength);
        if (level > 3) {
            return new Buffer(exceptLength);
        }

        Buffer buffer = buffers[level - 1].poll();
        if (buffer == null) {
            buffer = new Buffer(getSize(level));
        }
        return buffer;
    }

    public void returnBuffer(Buffer buffer) {
        int length = buffer.length();
        int level = getLevel(length);
        if (level <= 3) {
            buffer.reset();
            buffers[level - 1].offer(buffer);
        }
    }


    private static int getLevel(int size) {
        int level = 0;
        size /= 10;
        while (size > 0) {
            size /= 10;
            level++;
        }
        return level == 0 ? 1 : level;
    }

    private static int getSize(int level) {
        int size = 10;
        for (int i = 0; i < level; i++) {
            size *= 10;
        }
        return size;
    }



    static class Buffer extends ByteArrayOutputStream {
        public Buffer(int size) {
            super(size);
        }

        public int length() {
            return buf.length;
        }

        public byte[] array() {
            return buf;
        }

    }

}
