package com.example.nettygateway.server;

import com.example.nettygateway.handler.Jt808BusinessHandler;
import com.example.nettygateway.util.Jt808Decoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class NettyServer {

    @Value("${netty.port:8080}")
    private int port;

    private final Jt808BusinessHandler businessHandler;

    // 使用通用的 EventLoopGroup 接口
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;

    public NettyServer(Jt808BusinessHandler businessHandler) {
        this.businessHandler = businessHandler;
    }

    @PostConstruct
    public void start() {
        new Thread(() -> {
            // --- 4.2.x 新写法：使用 MultiThreadIoEventLoopGroup 配合 NioIoHandler 工厂 ---
            bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
            workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

            try {
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .option(ChannelOption.SO_BACKLOG, 1024)
                        .childOption(ChannelOption.SO_KEEPALIVE, true)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                ChannelPipeline p = ch.pipeline();
                                p.addLast(new DelimiterBasedFrameDecoder(1024, Unpooled.copiedBuffer(new byte[]{0x7e})));
                                p.addLast(new Jt808Decoder());
                                p.addLast(businessHandler);
                            }
                        });

                ChannelFuture f = b.bind(port).sync();
                this.channel = f.channel();

                log.info(">>> [Netty 4.2] 网关已启动，监听端口:  {}", port);

                f.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                stop();
            }
        }).start();
    }

    @PreDestroy
    public void stop() {
        if (channel != null) channel.close();
        if (bossGroup != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();

        log.info(">>> 网关服务已优雅关闭");
    }
}