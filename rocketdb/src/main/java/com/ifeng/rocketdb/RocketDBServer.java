package com.ifeng.rocketdb;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/**
 * Created by lijs
 * on 2017/7/31.
 */
public class RocketDBServer {
    static boolean linux = false;

    public static void main(String[] args) {
        if (args.length > 0 && Objects.equals(args[0], "linux")) {
            linux = true;
        }
        int port = 9999;
        new RocketDBServer().startServer(port);
    }

    private void startServer(int port) {
        ServerBootstrap server = new ServerBootstrap();
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(16);
        RocketDB db = null;

        try {
            server.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(port)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)//TODO:如何配置？
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            ChannelPipeline pipeline = socketChannel.pipeline();
                            pipeline.addFirst("decoder", new RocketProtocolDecoder());

                            //pipeline.addLast("encoder", new RocketProtocolEncoder());
                        }
                    });
            db = RocketDB.getInstance();
            Properties properties = new Properties();
            try {
                if (linux) {
                    properties.load(RocketDBServer.class.getResourceAsStream("/linux.properties"));

                } else {
                    properties.load(new FileInputStream("J:\\work\\ifeng\\rocks-java\\src\\main\\resources\\win.properties"));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            db.init(properties);

            System.out.println("Netty server has started on port : " + port);
            final RocketDB finalDb = db;
            server.bind().sync().channel().closeFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    finalDb.shutdown();
                }
            }).sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
