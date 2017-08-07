package com.ifeng.rocketdb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;

/**
 * Created by lijs
 * on 2017/7/31.
 */
public abstract class Task implements Runnable {

    public static RocketDB rocketDB = RocketDB.getInstance();
    public static final UnpooledByteBufAllocator ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;

    public Channel channel;
    public byte[] key;
    public long startTime;

    public Task(Channel channel, byte[] key) {
        startTime = System.nanoTime() / 1000L;
        this.channel = channel;
        this.key = key;
    }



    public void putLength(ByteBuf buf, int length, int charLen) {
        while (charLen > 0) {
            buf.writeByte(length / divideTable[--charLen] + '0');
            length = length % divideTable[charLen];
        }
    }

    private final static int [] sizeTable = { 9, 99, 999, 9999, 99999, 999999, 9999999,
            99999999, 999999999, Integer.MAX_VALUE };
    private final static int[] divideTable ={
            1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
    };
    // Requires positive x
    protected static int stringSize(int x) {
        for (int i=0; ; i++)
            if (x <= sizeTable[i])
                return i+1;
    }

}
