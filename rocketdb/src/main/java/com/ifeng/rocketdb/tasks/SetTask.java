package com.ifeng.rocketdb.tasks;

import com.ifeng.rocketdb.Task;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.rocksdb.RocksDBException;

import static com.ifeng.rocketdb.Constants.RESPONSE_FAIL;
import static com.ifeng.rocketdb.Constants.RESPONSE_OK2;

/**
 * Created by lijs
 * on 2017/7/31.
 */
public class SetTask extends Task {
    public byte[] val;

    public SetTask(Channel channel, byte[] key, byte[] val) {
        super(channel, key);
        this.key = key;
        this.val = val;
    }

    @Override
    public void run() {
        try {
            rocketDB.set(key, val);
            channel.writeAndFlush(Unpooled.wrappedBuffer(RESPONSE_OK2));
        } catch (RocksDBException e) {
            channel.writeAndFlush(Unpooled.wrappedBuffer(RESPONSE_FAIL));
            e.printStackTrace();
        }
    }
}
