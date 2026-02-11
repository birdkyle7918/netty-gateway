package com.example.nettygateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class NettyGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(NettyGatewayApplication.class, args);
    }

}
