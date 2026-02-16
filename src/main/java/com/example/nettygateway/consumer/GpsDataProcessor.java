package com.example.nettygateway.consumer;

import com.alibaba.fastjson2.JSON;
import com.example.nettygateway.gateway.dto.VehicleLocation;
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

/**
 * GPS 数据消费者
 * 核心职责：从 Kafka 接收数据 -> 放入本地内存“蓄水池” -> 攒够一批或时间到了 -> 批量写入 ClickHouse
 */
@Service
@Slf4j
public class GpsDataProcessor {

    private final JdbcTemplate jdbcTemplate;

    // 【本地蓄水池】
    // 这是一个线程安全的阻塞队列，相当于一个“候车厅”。
    // Kafka 发来的消息先暂存在这里，不直接写库，防止把数据库打挂。
    // 注意：new LinkedBlockingQueue<>() 默认容量是 Integer.MAX_VALUE (无界)，但在逻辑上我们把它当做有界来处理。
    private final BlockingQueue<VehicleLocation> buffer = new LinkedBlockingQueue<>();

    // 【发车阈值】
    // 就像大巴车，坐满 50000 人就发车。
    // ClickHouse 官方建议每批次写入 1000~100000 条数据性能最好。
    private static final int BATCH_SIZE = 50000;

    public GpsDataProcessor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // ==========================================
    // 1. 进水口：监听 Kafka 消息
    // ==========================================
    @KafkaListener(topics = "vehicle_gps", groupId = "gps_group")
    public void listen(String message) {
        VehicleLocation data;
        try {
            // 1.1 将 JSON 字符串“翻译”成 Java 对象
            data = JSON.parseObject(message, VehicleLocation.class);
        } catch (Exception e) {
            log.error("解析消息失败: {}", e.getMessage());
            return; // 脏数据直接丢弃，继续处理下一条
        }

        // 1.2 【尝试入队】
        // 尝试把乘客（数据）塞进候车厅（队列）。
        // offer 方法是非阻塞的：如果队列满了，它会立刻返回 false，不会傻等。
        boolean success = buffer.offer(data);

        // 1.3 【背压机制 / 紧急处理】
        // 如果候车厅爆满了 (success == false)，说明数据库写入太慢了，消费速度 > 写入速度。
        if (!success) {
            log.warn(">>> [Warning] 候车厅已满，触发紧急同步发车！");

            // 策略 A：【手动发车】
            // 这里直接在当前线程执行 flush()。
            // 注意：这会阻塞当前 Kafka 消费者线程，迫使 Kafka 暂停拉取新消息。
            // 这就是所谓的“背压” (Backpressure) —— 下游处理不过来时，要反向拖慢上游。
            flush();

            // 冲刷完后，候车厅空了，再次把刚才那条没挤进去的数据放进去
            try {
                // put 是阻塞方法：如果还满（理论上刚 flush 完不会满），就死等，直到放进去为止。
                buffer.put(data);
            } catch (InterruptedException e) {
                // 如果等待过程被打断，恢复中断状态，遵守线程规范
                Thread.currentThread().interrupt();
            }
        }

        // 1.4 【定量触发】
        // 如果候车厅的人数达到了 50000 人
        if (buffer.size() >= BATCH_SIZE) {
            // 【异步发车】
            // 叫一辆新的大巴车（开启新线程）去送人，不要耽误当前负责“拉客”的 Kafka 线程。
            // CompletableFuture.runAsync 会利用公共线程池执行 flush。
            CompletableFuture.runAsync(this::flush);
        }
    }

    // ==========================================
    // 2. 兜底机制：定时发车
    // ==========================================
    // 防止流量低谷时，数据攒不够 50000 条一直卡在内存里不入库。
    // 逻辑：不管有没有坐满，每隔 2 秒钟，强制发一趟车。
    @Scheduled(fixedRate = 2000)
    public void scheduledFlush() {
        flush();
    }

    // ==========================================
    // 3. 出水口：批量写入数据库
    // ==========================================
    // synchronized 关键字非常关键：
    // 保证同一时刻只有一辆“大巴车”在装人。
    // 因为 listen 线程、异步线程、定时任务线程都可能调用这个方法，必须加锁防止多线程并发抢占队列数据。
    private synchronized void flush() {
        // 如果候车厅是空的，直接返回，别跑空车
        if (buffer.isEmpty()) return;

        // 3.1 【准备大巴车】
        // 创建一个临时 List，用来装载这一批次的数据
        List<VehicleLocation> drainList = new ArrayList<>();

        // 3.2 【极速装车】
        // drainTo 是原子操作，效率极高。
        // 它会瞬间把 buffer 里的所有数据（或者指定数量）转移到 drainList 中，并清空 buffer。
        // 这比一个一个 poll 出来快得多。
        buffer.drainTo(drainList);

        // 3.3 【发往 ClickHouse】
        String sql = "INSERT INTO vehicle_gps (deviceId, timestamp, longitude, latitude) VALUES (?, ?, ?, ?)";

        // 使用 JDBC 的批量处理功能，一次网络 IO 发送几万条 SQL，极大提高性能
        jdbcTemplate.batchUpdate(sql, drainList, drainList.size(), (PreparedStatement ps, VehicleLocation data) -> {
            ps.setString(1, data.getDeviceId());
            // ClickHouse 的 DateTime 通常是秒级，Java 里的 System.currentTimeMillis() 是毫秒，所以要除以 1000
            ps.setLong(2, data.getTimestamp() / 1000);
            ps.setDouble(3, data.getLongitude());
            ps.setDouble(4, data.getLatitude());
        });

        // (注释掉的日志：生产环境如果每秒打印可能刷屏，建议保留 debug 级别或统计日志)
        // log.info(">>> [ClickHouse] 成功批量写入 {} 条轨迹数据", drainList.size());
    }
}