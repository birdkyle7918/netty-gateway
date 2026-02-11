package com.example.nettygateway.util;


import com.example.nettygateway.dto.VehicleLocation;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

/**
 * 协议解析器：负责将 ByteBuf 解析为 VehicleLocation 对象
 * 注意：Decoder 是有状态的，不能用 @Sharable，必须每次 new
 */
public class Jt808Decoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // 假设经过 DelimiterBasedFrameDecoder 后，报文已经去掉了头尾的 0x7E
        // 模拟结构：[2字节消息ID] [6字节BCD手机号] [4字节纬度] [4字节经度] ...

        // 简单的长度校验 (JT808 最小包长不止16，这里是示例)
        if (in.readableBytes() < 16) {
            return;
        }

        // 1. 跳过消息 ID (2 bytes)
        in.skipBytes(2);

        // 2. 读取终端手机号 (6字节 BCD 码 -> 12位字符串)
        byte[] bcdBytes = new byte[6];
        in.readBytes(bcdBytes);
        String deviceId = bcd2String(bcdBytes);

        // 3. 读取纬度 (4字节整数，单位：百万分之一度)
        double lat = in.readInt() / 1_000_000.0;

        // 4. 读取经度
        double lon = in.readInt() / 1_000_000.0;

        // 5. 生成对象传递给下一个 Handler
        out.add(new VehicleLocation(deviceId, lat, lon));
    }

    // BCD转String 工具方法 (8421码)
    private String bcd2String(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(b >> 4 & 0x0F).append(b & 0x0F);
        }
        return sb.toString();
    }
}