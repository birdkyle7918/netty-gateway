package com.example.nettygateway.simulator;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Jt808Simulator {

    private static final String HOST = "38.147.176.216";
    private static final int PORT = 8090;
    private static final int VEHICLE_COUNT = 10000; // 目标 2 万台
    private static final int INTERVAL_SEC = 5;

    // 状态统计
    private static final AtomicInteger connectedCount = new AtomicInteger(0);

    public static void main(String[] args) {
        // 对于 2 万连接，1 个 Boss 线程，Worker 线程设为 CPU 核心数即可
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(Runtime.getRuntime().availableProcessors(), NioIoHandler.newFactory());

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    // 关键优化：使用池化内存分配器，极大减少内存碎片
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 保持简洁，不需要任何 Handler
                        }
                    });

            System.out.println(">>> 准备模拟 " + VEHICLE_COUNT + " 台车辆，开始平滑建立连接...");

            for (int i = 0; i < VEHICLE_COUNT; i++) {
                final String deviceId = String.format("%012d", 13800000000L + i);
                // 预先计算好 BCD 码，避免上报时重复计算 CPU 开销
                final byte[] deviceIdBcd = stringToBcd(deviceId);

                b.connect(HOST, PORT).addListener((ChannelFuture future) -> {
                    if (future.isSuccess()) {
                        int current = connectedCount.incrementAndGet();
                        if (current % 1000 == 0) {
                            System.out.println(">>> 已成功建立连接数: " + current);
                        }
                        // 连接成功后，随机延迟 0-5 秒开始上报，打散流量
                        startStaggeredReporting(future.channel(), deviceIdBcd);
                    }
                });

                // 启动风暴控制：每建立 50 个连接休息 100ms
                if (i % 50 == 0) {
                    Thread.sleep(500);
                }
            }

            // 保持主线程
            Thread.currentThread().join();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }

    /**
     * 打散上报时间：防止所有车辆在同一秒齐射
     */
    private static void startStaggeredReporting(Channel channel, byte[] deviceIdBcd) {
        // 生成一个 0 到 INTERVAL_SEC 之间的随机毫秒数
        long initialDelay = ThreadLocalRandom.current().nextLong(INTERVAL_SEC * 1000L);

        channel.eventLoop().scheduleAtFixedRate(() -> {
            if (channel.isActive()) {
                // 使用分配器获取 ByteBuf
                ByteBuf packet = generateOptimizedPacket(channel.alloc(), deviceIdBcd);
                channel.writeAndFlush(packet);
            }
        }, initialDelay, INTERVAL_SEC * 1000L, TimeUnit.MILLISECONDS);
    }

    /**
     * 极致优化的报文生成
     */
    private static ByteBuf generateOptimizedPacket(ByteBufAllocator allocator, byte[] bcdId) {
        // JT808 位置汇报包通常固定 16 字节
        ByteBuf buf = allocator.buffer(16);
        buf.writeByte(0x7E);             // 标识
        buf.writeShort(0x0002);          // 消息 ID
        buf.writeBytes(bcdId);           // 预计算好的终端手机号 (6字节)

        // 模拟坐标变化
        int lat = (int) ((28.2 + ThreadLocalRandom.current().nextDouble(0.1)) * 1_000_000);
        int lon = (int) ((112.9 + ThreadLocalRandom.current().nextDouble(0.1)) * 1_000_000);
        buf.writeInt(lat);
        buf.writeInt(lon);

        buf.writeByte(0x7E);             // 标识
        return buf;
    }

    private static byte[] stringToBcd(String s) {
        int len = s.length();
        byte[] bcd = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            bcd[i / 2] = (byte) ((Integer.parseInt(s.substring(i, i + 1)) << 4)
                    | Integer.parseInt(s.substring(i + 1, i + 2)));
        }
        return bcd;
    }
}