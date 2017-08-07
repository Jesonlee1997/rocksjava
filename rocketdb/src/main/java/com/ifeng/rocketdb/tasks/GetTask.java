package com.ifeng.rocketdb.tasks;

import com.ifeng.rocketdb.Task;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.rocksdb.RocksDBException;

import static com.ifeng.rocketdb.Constants.RESPONSE_FAIL;
import static com.ifeng.rocketdb.Constants.RESPONSE_NOT_FOUND;
import static com.ifeng.rocketdb.Constants.RESPONSE_OK;

/**
 * Created by lijs
 * on 2017/7/31.
 */
public class GetTask extends Task {


    public GetTask(Channel channel, byte[] key) {
        super(channel, key);
    }

    @Override
    public void run() {
        ByteBuf buf = null;
        try {
            byte[] res = rocketDB.get(key);
            if (res == null) {
                channel.writeAndFlush(Unpooled.wrappedBuffer(RESPONSE_NOT_FOUND));
                return;
            }
            int charLen = stringSize(res.length);
            buf = ALLOCATOR.directBuffer(res.length + RESPONSE_OK.length + charLen + 3);
            buf.writeBytes(RESPONSE_OK);
            putLength(buf, res.length, charLen);
            buf.writeByte('\n');
            buf.writeBytes(res);
            buf.writeByte('\n');
            buf.writeByte('\n');
            channel.writeAndFlush(buf);
        } catch (RocksDBException e) {
            if (buf != null && buf.refCnt() == 1) {
                buf.release();
            }
            buf = Unpooled.wrappedBuffer(RESPONSE_FAIL);
            channel.writeAndFlush(buf);
            e.printStackTrace();
        }

    }
}
