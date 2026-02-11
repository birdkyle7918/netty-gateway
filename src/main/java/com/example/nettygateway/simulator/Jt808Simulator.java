package com.example.nettygateway.simulator;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.TimeUnit;

public class Jt808Simulator {

    // 配置参数
    private static final String HOST = "38.147.176.216"; // 网关 IP
    private static final int PORT = 8090;          // 网关端口
    private static final int VEHICLE_COUNT = 10;  // 模拟车辆数
    private static final int INTERVAL_SEC = 5;     // 上报间隔

    public static void main(String[] args) {
        // 使用 4.2.x 推荐的 MultiThreadIoEventLoopGroup
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 模拟器比较简单，不需要复杂的 Pipeline
                        }
                    });

            System.out.println(">>> 正在启动 " + VEHICLE_COUNT + " 个模拟连接...");

            for (int i = 0; i < VEHICLE_COUNT; i++) {
                // 为每辆车生成一个模拟手机号 (13800000000 + i)
                String deviceId = String.format("%012d", 13800000000L + i);

                // 发起连接
                b.connect(HOST, PORT).addListener((ChannelFuture future) -> {
                    if (future.isSuccess()) {
                        // 连接成功后，开启定时任务上报数据
                        scheduleReporting(future.channel(), deviceId);
                    }
                });

                // 稍微错开启动时间，避免瞬间冲击自己电脑的网卡
                if (i % 100 == 0) Thread.sleep(100);
            }

            // 保持主线程运行
            Thread.currentThread().join();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }

    private static void scheduleReporting(Channel channel, String deviceId) {
        channel.eventLoop().scheduleAtFixedRate(() -> {
            if (channel.isActive()) {
                ByteBuf packet = generateJt808Packet(deviceId);
                channel.writeAndFlush(packet);
            }
        }, 0, INTERVAL_SEC, TimeUnit.SECONDS);
    }

    /**
     * 构造符合网关解析逻辑的二进制包
     * 格式：7E + MsgID(2) + Mobile(6 BCD) + Lat(4) + Lon(4) + 7E
     */
    private static ByteBuf generateJt808Packet(String deviceId) {
        ByteBuf buf = Unpooled.buffer(16);
        buf.writeByte(0x7E); // 头
        buf.writeShort(0x0002); // 消息ID: 位置信息汇报

        // 手机号 BCD 码 (6字节)
        buf.writeBytes(stringToBcd(deviceId));

        // 模拟随机经纬度 (以长沙为例：28.2, 112.9)
        int latitude = (int) ((28.2 + Math.random() * 0.1) * 1_000_000);
        int longitude = (int) ((112.9 + Math.random() * 0.1) * 1_000_000);
        buf.writeInt(latitude);
        buf.writeInt(longitude);

        buf.writeByte(0x7E); // 尾
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
