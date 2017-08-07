package com.ifeng.rocketdb;

import com.ifeng.rocketdb.tasks.DelTask;
import com.ifeng.rocketdb.tasks.GetTask;
import com.ifeng.rocketdb.tasks.SetTask;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.IOException;
import java.util.concurrent.Executor;

import static com.ifeng.rocketdb.Constants.*;

/**
 * Created by lijs
 * on 2017/7/31.
 */
public class RocketProtocolDecoder extends ChannelInboundHandlerAdapter {
    private Channel channel;

    private Executor workers = RocketDB.getInstance().getWorkers();
    private BytePool pool = BytePool.getInstance();
    private BytePool.Buffer buffer;
    private byte[] bytes = new byte[65536];//TODO:缓冲数组接收buf中的字节
    private Status status = new Status();

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        channel = ctx.channel();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf data = (ByteBuf) msg;

            readFrom(data);

            if (!status.finished) {
                return;
            }

            Task task = getTaskFromBytes(buffer.toByteArray());

            returnBuffer(buffer);

            workers.execute(task);
        }
    }


    private void returnBuffer(BytePool.Buffer buffer) {
        pool.returnBuffer(buffer);
        this.buffer = null;
    }

    //TODO
    private void readFrom(ByteBuf buf) throws IOException {
        int readable = buf.readableBytes();

        if (buffer == null) {
            buffer = pool.getBuffer(readable);
        }
        buf.readBytes(bytes, 0, readable);

        byte b;
        StringBuilder builder = status.builder;
        int blockStatus = status.blockStatus;
        int blockLength = status.blockLength;
        int dataCount = status.dataCount;

        for (int i = 0; i < readable - 1; i++) {
            b = bytes[i];
            if (b == '\n') {
                if (blockStatus == 0) {
                    blockLength = Integer.parseInt(builder.toString());
                    builder.setLength(0);
                    blockStatus = 1;
                } else {
                    if (dataCount > blockLength) {
                        if (bytes[i + 1] == '\n') {
                            buffer.write('\n');
                            buffer.write('\n');
                            status.reset();
                            return;//唯一正确的出口
                        }
                        buffer.write('\n');

                        dataCount = 0;
                        blockLength = 0;
                        blockStatus = 0;
                        continue;
                    }
                }
            }

            if (blockStatus == 0) {
                builder.append(b - '0');
            } else {
                dataCount++;
            }

            buffer.write(b);
        }

        //判断最后一个字节
        b = bytes[readable - 1];
        if (b == '\n') {
            if (blockStatus == 0) {
                blockLength = Integer.parseInt(builder.toString());
                builder.setLength(0);
                blockStatus = 1;
            } else {
                if (dataCount > blockLength) {
                    dataCount = 0;
                    blockLength = 0;
                    blockStatus = 0;
                }
            }
        }
        if (blockStatus == 0) {
            builder.append(b - '0');
        } else {
            dataCount++;
        }

        buffer.write(b);

        status.finished = false;//还有未接收的字节
        status.builder = builder;
        status.blockStatus = blockStatus;
        status.blockLength = blockLength;
        status.dataCount = dataCount;
    }

    private Task getTaskFromBytes(byte[] bytes) {
        int position;

        int optLength = bytes[0] - '0';
        int opt = 0;

        assert bytes[1] == '\n';

        for (position = 2; position < optLength + 2; position++) {
            opt += bytes[position];
        }

        assert bytes[position] == '\n';

        position++;

        int keyLength = 0;
        while (bytes[position] != '\n') {
            keyLength = keyLength * 10 + (bytes[position++] - '0');
        }

        position++;

        byte[] key = new byte[keyLength];
        for (int i = 0; i < keyLength; i++) {
            key[i] = bytes[position++];
        }

        assert bytes[position] == '\n';
        position++;
        Task task = null;
        //根据不同的请求解析
        switch (opt) {
            case OPT_GET:
                task = new GetTask(channel, key);
                break;
            case OPT_HGET:
                byte[] hkey;
                int hKeyLength = 0;
                while (bytes[position] != '\n') {
                    hKeyLength = hKeyLength * 10 + (bytes[position++] - '0');
                }

                position++;

                hkey = new byte[hKeyLength];
                for (int i = 0; i < hKeyLength; i++) {
                    hkey[i] = bytes[position++];
                }
                key = encodeHashKey(key, hkey);
                task = new GetTask(channel, key);

                assert bytes[position] == '\n';
                break;

            case OPT_SET:
                int valLength = 0;
                while (bytes[position] != '\n') {
                    valLength = valLength * 10 + (bytes[position++] - '0');
                }

                position++;

                byte[] val = new byte[valLength];
                for (int i = 0; i < valLength; i++) {
                    val[i] = bytes[position++];
                }
                task = new SetTask(channel, key, val);

                assert bytes[position] == '\n';
                break;
            case OPT_HSET:
                hKeyLength = 0;
                while (bytes[position] != '\n') {
                    hKeyLength = hKeyLength * 10 + (bytes[position++] - '0');
                }

                position++;

                hkey = new byte[hKeyLength];
                for (int i = 0; i < hKeyLength; i++) {
                    hkey[i] = bytes[position++];
                }

                assert bytes[position] == '\n';
                position++;

                valLength = 0;
                while (bytes[position] != '\n') {
                    valLength = valLength * 10 + (bytes[position++] - '0');
                }

                position++;

                val = new byte[valLength];
                for (int i = 0; i < valLength; i++) {
                    val[i] = bytes[position++];
                }

                key = encodeHashKey(key, hkey);
                task = new SetTask(channel, key, val);

                assert bytes[position] == '\n';
                break;
            case OPT_DEL:
                task = new DelTask(channel, key);
                break;

            case OPT_HDEL:
                hKeyLength = 0;
                while (bytes[position] != '\n') {
                    hKeyLength = hKeyLength * 10 + (bytes[position++] - '0');
                }

                position++;

                hkey = new byte[hKeyLength];
                for (int i = 0; i < hKeyLength; i++) {
                    hkey[i] = bytes[position++];
                }
                key = encodeHashKey(key, hkey);
                task = new DelTask(channel, key);

                assert bytes[position] == '\n';

        }
        assert bytes[position] == '\n';
        return task;
    }

    private byte[] encodeHashKey(byte[] key, byte[] hKey) {
        int newKeyLength = key.length + hKey.length + 2;
        byte[] bytes = new byte[newKeyLength];

        System.arraycopy(key, 0, bytes, 0, key.length);
        int position = key.length;
        bytes[position++] = '=';
        bytes[position++] = '?';
        System.arraycopy(hKey, 0, bytes, position, hKey.length);

        return bytes;
    }

    class Status {
        StringBuilder builder = new StringBuilder();
        int blockStatus = 0;
        boolean finished = true;
        int blockLength = 0;
        int dataCount = 0;

        void reset() {
            builder.setLength(0);
            blockStatus = 0;
            finished = true;
            blockLength = 0;
            dataCount = 0;
        }
    }
}
