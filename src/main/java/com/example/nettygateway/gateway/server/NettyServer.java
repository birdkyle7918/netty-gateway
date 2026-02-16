package com.example.nettygateway.gateway.server;

import com.example.nettygateway.gateway.handler.Jt808BusinessHandler;
import com.example.nettygateway.util.Jt808Decoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
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
 * Netty 服务端主程序
 * <p>
 * 核心职责：
 * 1. 启动 TCP 服务监听端口。
 * 2. 组装“流水线”（Pipeline），规定数据包进来后要经过哪些步骤处理。
 * 3. 监控连接数量和 QPS（每秒处理消息数）。
 * 4. 配合 Spring Boot 实现优雅停机。
 * </p>
 */
@Slf4j
@Component
public class NettyServer {

    // ================= 全局容器 =================

    /**
     * 【在线点名册】
     * 用于存放所有处于“活跃状态”的客户端连接。
     * GlobalEventExecutor.INSTANCE 是一个单线程的执行器，用于自动清理断开的连接。
     * 作用：如果你想群发消息，或者统计当前有多少人在线，直接查这个 Group 即可。
     */
    public static final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    /**
     * 【总消息计数器】
     * 使用 LongAdder 而不是 AtomicLong，因为在高并发写入下，LongAdder 性能更高（减少了乐观锁冲突）。
     * 作用：记录服务器从启动到现在，一共收到了多少个包。
     */
    public static final LongAdder totalMessageCount = new LongAdder();

    /**
     * 【监控定时器】
     * 一个单线程的调度池，专门用来每秒打印一次日志，看看系统负载。
     */
    private final ScheduledExecutorService monitorExecutor = Executors.newSingleThreadScheduledExecutor();

    // ================= 配置参数 =================

    // 从 application.properties 读取端口，如果没写，默认用 8090
    @Value("${netty.port:8090}")
    private int port;

    // ================= 核心组件 =================

    /**
     * 业务逻辑处理器
     * 这是流水线的“最后一站”，真正处理业务（比如存数据库、回复消息）的地方。
     * 通过构造函数注入，保证它可以使用 Spring 的 Service/Dao。
     */
    private final Jt808BusinessHandler businessHandler;

    /**
     * 【Boss 线程组】（迎宾大爷）
     * 只负责一件事：处理 TCP 连接请求 (Accept)。
     * 只要客户端连上了，Boss 就会把它转交给 Worker。
     */
    private EventLoopGroup bossGroup;

    /**
     * 【Worker 线程组】（车间工人）
     * 负责处理所有已连接的 Socket 的 IO 读写、编解码、业务执行。
     * 哪怕有 10000 个连接，也由这组固定的线程（通常是 CPU 核数 * 2）轮询处理。
     */
    private EventLoopGroup workerGroup;

    // 保存服务端的 Channel 对象，用于停机时关闭
    private Channel channel;

    /**
     * 构造函数注入
     */
    public NettyServer(Jt808BusinessHandler businessHandler) {
        this.businessHandler = businessHandler;
    }

    /**
     * 【核心启动方法】
     * 加上 @PostConstruct 注解，表示 Spring 容器初始化完这个 Bean 后，自动执行该方法。
     */
    @PostConstruct
    public void start() {
        // 1. 先把监控任务跑起来
        startMonitorTask();

        // 2. 开启一个新线程来启动 Netty
        // 【注意】这里必须开新线程！否则 Netty 的 channel().closeFuture().sync() 会阻塞住主线程，
        // 导致 Spring Boot 永远卡在启动阶段，Tomcat 也起不来。
        new Thread(() -> {
            // --- Netty 4.2.x 新特性写法 ---
            // 使用 MultiThreadIoEventLoopGroup 配合 NioIoHandler，这是 Netty 新版推荐的高性能模型
            // 参数 1：Boss 只需要 1 个线程（一个端口监听只要一个人就够了）
            bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
            // 参数默认：Worker 线程数默认为 CPU 核心数 * 2
            workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

            try {
                // 引导类：用于配置服务端的启动参数
                ServerBootstrap b = new ServerBootstrap();
                b.group(bossGroup, workerGroup) // 绑定两个线程组
                        .channel(NioServerSocketChannel.class) // 指定使用 NIO 传输模型
                        // 【TCP 参数：全连接队列大小】
                        // 如果一瞬间有大量连接涌入，Worker 处理不过来，操作系统可以挂起 10240 个连接等待处理。
                        .option(ChannelOption.SO_BACKLOG, 10240)
                        // 【TCP 参数：心跳保活】
                        // 开启操作系统底层的 TCP KeepAlive，检测死链接（注意这和业务心跳是两码事）。
                        .childOption(ChannelOption.SO_KEEPALIVE, true)

                        // ================= 定义流水线 (Pipeline) =================
                        // 每一个新连接进来，都会按照下面的步骤，组装属于它的处理链条
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                // 获取该连接的流水线对象
                                ChannelPipeline p = ch.pipeline();

                                // --- 第 1 步：连接登记 ---
                                // 当连接建立成功(active)时，自动把它加入到全局点名册(allChannels)
                                p.addLast(new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelActive(ChannelHandlerContext ctx) {
                                        NettyServer.allChannels.add(ctx.channel());
                                        // 这里的 fireChannelActive 很重要，表示把“连接建立”这个事件继续传下去
                                        // 如果不写，后面的 Handler 可能就不知道连接建立成功了
                                        ctx.fireChannelActive();
                                    }
                                });

                                // --- 第 2 步：解决粘包/拆包 (核心物理层处理) ---
                                // 场景：TCP 是流式的，数据可能粘在一起。
                                // 方案：JT808 协议规定每条消息以 0x7e 开头，0x7e 结尾。
                                // DelimiterBasedFrameDecoder 会自动帮你把一堆乱七八糟的字节切成一个个完整的包。
                                // 1024 是单条消息最大长度限制，超过会报错。
                                p.addLast(new DelimiterBasedFrameDecoder(1024,
                                        Unpooled.copiedBuffer(new byte[]{0x7e})));


                                // --- 第 3 步：幽灵空包过滤器 (清洗数据) ---
                                // 场景：JT808 协议里，如果两个 0x7e 挨着（7E 7E），拆包器会切出一个长度为 0 的包。
                                // 这种包没有业务意义，必须丢弃，否则会干扰后续逻辑。
                                p.addLast(new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                        ByteBuf buf = (ByteBuf) msg;

                                        // 如果是 0 字节的空包
                                        if (buf.readableBytes() == 0) {
                                            // 【重要】Netty 使用引用计数管理内存。
                                            // 如果你把消息拦截了（return 了），不传给后面，必须手动释放内存！
                                            // 否则会发生内存泄漏 (Memory Leak)。
                                            buf.release();
                                            return; // 直接拦截，不往下传
                                        }

                                        // 如果是正常包，继续往下传
                                        ctx.fireChannelRead(msg);
                                    }
                                });

                                // --- 第 4 步：流量统计 (监控) ---
                                // 代码能走到这里，说明是一个“非空”且“完整”的数据包。
                                p.addLast(new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                        // 计数器 +1
                                        totalMessageCount.increment();

                                        // 继续传递给下一个 Handler (解码器)
                                        ctx.fireChannelRead(msg);
                                    }
                                });

                                // --- 第 5 步：协议解码 (翻译官) ---
                                // 将看不懂的字节流 (ByteBuf) 翻译成 Java 对象 (Jt808Message)
                                p.addLast(new Jt808Decoder());

                                // --- 第 6 步：业务处理 (大管家) ---
                                // 拿到翻译好的 Java 对象，执行具体的逻辑（如：保存定位、回复应答）
                                p.addLast(businessHandler);
                            }
                        });

                // 3. 绑定端口，开始监听
                // sync() 表示阻塞当前线程，直到绑定成功为止
                ChannelFuture f = b.bind(port).sync();
                this.channel = f.channel();

                log.info(">>> [Netty 4.2] 网关已启动，监听端口: {}", port);

                // 4. 等待服务端监听端口关闭
                // 这行代码会一直阻塞（卡住），直到服务器被关闭。
                // 因为我们在独立线程里运行，所以不会卡住 Spring Boot。
                f.channel().closeFuture().sync();

            } catch (InterruptedException e) {
                log.error("Netty 服务启动被中断", e);
                // 恢复线程的中断状态，这是一种良好的编程习惯
                Thread.currentThread().interrupt();
            } finally {
                // 如果运行到这里，说明 Server 挂了或者退出了，执行清理工作
                stop();
            }
        }).start();
    }

    /**
     * 【监控任务】
     * 这是一个每秒执行一次的定时任务，用来计算实时吞吐量 (QPS)。
     */
    private void startMonitorTask() {
        monitorExecutor.scheduleAtFixedRate(new Runnable() {
            // 记录上一秒的总数，用来做减法计算增量
            private long lastTotalCount = 0;

            @Override
            public void run() {
                try {
                    // 获取当前总消息数
                    long currentTotalCount = totalMessageCount.sum();
                    // 这一秒收到了多少 = 当前总数 - 上一秒的总数
                    long qps = currentTotalCount - lastTotalCount;
                    // 更新游标
                    lastTotalCount = currentTotalCount;

                    // 只有当有流量，或者有连接在线时，才打印日志，避免日志刷屏
                    if (qps > 0 || !allChannels.isEmpty()) {
                        log.info(">>> [监控看板] 在线设备: {} 台, 实时 QPS: {} msg/s, 累计接收包: {}",
                                allChannels.size(), qps, currentTotalCount);
                    }
                } catch (Exception e) {
                    log.error("监控线程发生异常", e);
                }
            }
        }, 1, 1, TimeUnit.SECONDS); // 初始延迟1秒，之后每隔1秒执行一次
    }

    /**
     * 【优雅停机】
     * 当 Spring 容器销毁（例如 Kill -15 或点击 IDE 停止按钮）时，会调用此方法。
     * 作用：保证先把手头的活儿干完，再释放资源，防止数据丢失。
     */
    @PreDestroy
    public void stop() {
        // 1. 关闭端口监听
        if (channel != null) {
            channel.close();
        }
        // 2. 优雅关闭 Boss 线程组（拒绝新连接）
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        // 3. 优雅关闭 Worker 线程组（处理完现有数据的尾巴再关闭）
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        log.info(">>> 网关服务已执行优雅关闭，资源已释放。");
    }
}