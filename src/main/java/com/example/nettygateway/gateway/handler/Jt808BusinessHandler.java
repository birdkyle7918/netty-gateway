package com.example.nettygateway.gateway.handler;

import com.alibaba.fastjson2.JSON;
import com.example.nettygateway.gateway.dto.VehicleLocation;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@Component
@ChannelHandler.Sharable
@Slf4j
public class Jt808BusinessHandler extends SimpleChannelInboundHandler<VehicleLocation> {

    private final KafkaTemplate<String, String> kafkaTemplate;

    // --- 【核心修改 1】高性能累加器 ---
    // 用于统计“尝试发送”的次数
    private final LongAdder kafkaSendCounter = new LongAdder();

    // (可选) 用于统计“发送成功”的次数 - 需要在 callback 里处理，会增加一点点 CPU 开销
    // private final LongAdder kafkaSuccessCounter = new LongAdder();

    @Autowired
    public Jt808BusinessHandler(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * --- 【核心修改 2】启动一个后台监控线程 ---
     * 每秒钟打印一次当前的发送速率
     */
    @PostConstruct
    public void startMonitor() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new Runnable() {
            private long lastTotal = 0;

            @Override
            public void run() {
                long currentTotal = kafkaSendCounter.sum();
                long qps = currentTotal - lastTotal; // 计算这一秒的增量
                lastTotal = currentTotal;

                if (qps > 0) {
                    log.info(">>> [Kafka监控] 写入 QPS: {} msg/s (累计: {})", qps, currentTotal);
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, VehicleLocation msg) {
        try {
            // 1. 序列化 (这是 CPU 密集型操作，放在这里做没问题)
            String jsonMsg = JSON.toJSONString(msg);

            // 2. --- 【核心修改 3】计数器 +1 ---
            // 这一步耗时极短（纳秒级），完全不会阻塞 Netty 线程
            kafkaSendCounter.increment();

            // 3. 发送 Kafka (异步操作)
            // 注意：这是“异步发送”，只要本地缓冲区没满，这里会瞬间返回
            kafkaTemplate.send("vehicle_gps", msg.getDeviceId(), jsonMsg);

            // 如果你想统计“发送成功”的 QPS，可以使用回调：
            /*
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    kafkaSuccessCounter.increment();
                } else {
                    log.error("Kafka发送失败", ex);
                }
            });
            */

        } catch (Exception e) {
            log.error("业务处理异常", e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Netty异常: {}", cause.getMessage());
        ctx.close();
    }
}