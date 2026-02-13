package com.example.nettygateway.consumer;

import com.alibaba.fastjson2.JSON;
import com.example.nettygateway.gateway.dto.VehicleLocation;
import com.example.nettygateway.gateway.server.NettyServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

@Service
@Slf4j
public class GpsDataProcessor {

    private final JdbcTemplate jdbcTemplate;
    // 本地缓冲区，使用阻塞队列保证线程安全
    private final BlockingQueue<VehicleLocation> buffer = new LinkedBlockingQueue<>();

    // 阈值配置
    private static final int BATCH_SIZE = 50000;

    public GpsDataProcessor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // 1. 监听 Kafka 消息，存入缓冲区
    @KafkaListener(topics = "vehicle_gps", groupId = "gps_group")
    public void listen(String message) {
        VehicleLocation data;
        try {
            data = JSON.parseObject(message, VehicleLocation.class);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return;
        }

        // 尝试将数据放入队列
        boolean success = buffer.offer(data);

        if (!success) {
            // 如果队列满了，说明 ClickHouse 写入太慢，产生背压
            log.warn(">>> [Warning] 队列已满，触发紧急同步冲刷！");

            // 策略 A：手动触发一次同步写入（会阻塞当前 Kafka 消费线程，起到限流作用）
            flush();

            // 冲刷完后再次尝试放入
            try {
                // 这次改用 put，如果还是满的，就让 Kafka 消费者等一下
                buffer.put(data);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 正常的定量触发逻辑
        if (buffer.size() >= BATCH_SIZE) {
            // 这里可以改为异步执行，不阻塞监听线程
            CompletableFuture.runAsync(this::flush);
        }
    }

    // 2. 定时任务：每 2 秒强制刷一次盘（即使没攒够 2000 条）
    @Scheduled(fixedRate = 2000)
    public void scheduledFlush() {
        flush();
    }

    // 3. 执行真正的 ClickHouse 批量写入
    private synchronized void flush() {
        if (buffer.isEmpty()) return;

        List<VehicleLocation> drainList = new ArrayList<>();
        buffer.drainTo(drainList); // 原子性取出所有数据

        String sql = "INSERT INTO vehicle_gps (deviceId, timestamp, longitude, latitude) VALUES (?, ?, ?, ?)";

        jdbcTemplate.batchUpdate(sql, drainList, drainList.size(), (PreparedStatement ps, VehicleLocation data) -> {
            ps.setString(1, data.getDeviceId());
            ps.setLong(2, data.getTimestamp() / 1000); // ClickHouse DateTime 支持秒级，DateTime64 支持毫秒
            ps.setDouble(3, data.getLongitude());
            ps.setDouble(4, data.getLatitude());
        });

//        log.info(">>> [ClickHouse] 成功批量写入 {} 条轨迹数据", drainList.size());
    }

    @Scheduled(fixedRate = 1000)
    public void debugConnections() {
        // 假设你把所有连接存到了 NettyServer.allChannels 里
        System.out.println(">>> 内存中真实连接数: " + NettyServer.allChannels.size());
    }
}