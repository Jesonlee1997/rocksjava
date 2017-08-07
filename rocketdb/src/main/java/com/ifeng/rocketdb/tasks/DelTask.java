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
public class DelTask extends Task {

    public DelTask(Channel channel, byte[] key) {
        super(channel, key);
    }

    @Override
    public void run() {
        try {
            rocketDB.del(key);
            channel.writeAndFlush(Unpooled.wrappedBuffer(RESPONSE_OK2));
        } catch (RocksDBException e) {
            channel.writeAndFlush(Unpooled.wrappedBuffer(RESPONSE_FAIL));
            e.printStackTrace();
        }
    }
}
