package com.example.nettygateway.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaTestController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaTestController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/send")
    public String send(@RequestParam String msg) {
        // 尝试发送到 test-topic
        kafkaTemplate.send("test-topic", msg);
        return "消息发送成功: " + msg;
    }
}