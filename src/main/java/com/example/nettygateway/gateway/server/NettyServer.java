package com.example.nettygateway.gateway.server;

import com.example.nettygateway.gateway.handler.Jt808BusinessHandler;
import com.example.nettygateway.util.Jt808Decoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Netty 服务端实现类
 * 负责网关的启动、初始化流水线（Pipeline）以及优雅停机
 */
@Slf4j
@Component
public class NettyServer {

    // allChannels，它是全局唯一的
    public static final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public static final LongAdder totalMessageCount = new LongAdder();

    private final ScheduledExecutorService monitorExecutor = Executors.newSingleThreadScheduledExecutor();

    // 从 application.properties/yml 获取端口，默认 8080
    @Value("${netty.port:8090}")
    private int port;

    // 注入自定义的业务处理器（处理解码后的 JT808 对象）
    private final Jt808BusinessHandler businessHandler;

    // Boss 线程组：负责接收客户端的连接请求
    private EventLoopGroup bossGroup;
    // Worker 线程组：负责处理已经建立连接的 Socket 读写逻辑
    private EventLoopGroup workerGroup;
    // 服务端的 Channel 引用
    private Channel channel;

    /**
     * 构造函数注入，确保 businessHandler 能够被 Spring 管理
     */
    public NettyServer(Jt808BusinessHandler businessHandler) {
        this.businessHandler = businessHandler;
    }

    /**
     * Bean 初始化后自动执行
     * 开启一个独立线程启动 Netty，避免阻塞 Spring Boot 主线程
     */
    @PostConstruct
    public void start() {

        startMonitorTask();

        new Thread(() -> {
            // --- 4.2.x 新写法：使用 MultiThreadIoEventLoopGroup 配合 NioIoHandler 工厂 ---
            // 这种方式是 Netty 4.2 推荐的，旨在提供更灵活的 IO 事件处理
            bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
            workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

            try {
                // 服务端引导程序
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class) // 指定 NIO 传输模式
                        .option(ChannelOption.SO_BACKLOG, 10240) // 设置全连接队列大小
                        .childOption(ChannelOption.SO_KEEPALIVE, true) // 开启 TCP 心跳探测
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                // 获取该连接的流水线
                                ChannelPipeline p = ch.pipeline();

                                // 增加这一段：监听连接状态
                                p.addLast(new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelActive(ChannelHandlerContext ctx) {
                                        // 当连接激活（建立成功）时，加入点名册
                                        NettyServer.allChannels.add(ctx.channel());
                                        // 继续把事件往后传给你的解码器
                                        ctx.fireChannelActive();
                                    }
                                });

                                // 1. 解决粘包/半包：JT/808 协议以 0x7e 开头和结尾
                                // 这里使用 DelimiterBasedFrameDecoder 按照 0x7e 进行切割
                                p.addLast(new DelimiterBasedFrameDecoder(1024,
                                        Unpooled.copiedBuffer(new byte[]{0x7e})));

                                // --- 【新增 4】流量统计 Handler (必须放在拆包器之后) ---
                                // 放在这里统计的才是“完整的一个包”，而不是 TCP 碎片
                                p.addLast(new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                        // 计数器 +1
                                        totalMessageCount.increment();
                                        // 继续传递给下一个 Handler
                                        ctx.fireChannelRead(msg);
                                    }
                                });

                                // 2. 协议解码器：将原始字节流（ByteBuf）转换为 JT808Message 实体类
                                p.addLast(new Jt808Decoder());

                                // 3. 业务处理器：处理登录、心跳、位置上报等具体业务
                                p.addLast(businessHandler);


                            }
                        });

                // 绑定端口并同步等待成功
                ChannelFuture f = b.bind(port).sync();
                this.channel = f.channel();

                log.info(">>> [Netty 4.2] 网关已启动，监听端口: {}", port);

                // 等待服务端连接关闭（此方法会阻塞线程，直到服务停止）
                f.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                log.error("Netty 服务启动异常", e);
                Thread.currentThread().interrupt(); // 保持中断状态
            } finally {
                stop(); // 异常或关闭时执行清理
            }
        }).start();
    }

    /**
     * 【新增 5】监控逻辑具体实现
     */
    private void startMonitorTask() {
        monitorExecutor.scheduleAtFixedRate(new Runnable() {
            private long lastTotalCount = 0;

            @Override
            public void run() {
                try {
                    // 获取当前总数
                    long currentTotalCount = totalMessageCount.sum();
                    // 计算这一秒内的增量 (即 QPS)
                    long qps = currentTotalCount - lastTotalCount;
                    // 更新上次的总数
                    lastTotalCount = currentTotalCount;

                    // 只有当有流量时才打印，或者一直打印以便观察心跳
                    if (qps > 0 || !allChannels.isEmpty()) {
                        log.info(">>> [监控] 当前连接数: {}, 实时 QPS: {} msg/s, 累计接收: {}",
                                allChannels.size(), qps, currentTotalCount);
                    }
                } catch (Exception e) {
                    log.error("监控线程异常", e);
                }
            }
        }, 1, 1, TimeUnit.SECONDS); // 初始延迟1秒，每隔1秒执行一次
    }

    /**
     * Bean 销毁前执行
     * 负责关闭连接并释放 Netty 线程池资源
     */
    @PreDestroy
    public void stop() {
        if (channel != null) {
            channel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(); // 优雅关闭，允许任务执行完再退出
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        log.info(">>> 网关服务已优雅关闭");
    }
}