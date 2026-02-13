package com.example.nettygateway.gateway.handler;

import com.alibaba.fastjson2.JSON;
import com.example.nettygateway.gateway.dto.VehicleLocation;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@ChannelHandler.Sharable
@Slf4j
public class Jt808BusinessHandler extends SimpleChannelInboundHandler<VehicleLocation> {

    // 使用 final 配合构造函数注入，这是 Spring 推荐的最佳实践
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public Jt808BusinessHandler(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, VehicleLocation msg) {
        try {
//            log.info(">>> [网关] 收到车辆数据: {}", JSON.toJSONString(msg));

            // 发送到 Kafka
            // Topic: vehicle_gps
            // Key: deviceId (保证同一辆车的数据进入同一个分区，保证顺序)
            // Value: JSON String (这里为了简单直接用了 toString)
            kafkaTemplate.send("vehicle_gps", msg.getDeviceId(), JSON.toJSONString(msg));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);
        ctx.close();
    }
}