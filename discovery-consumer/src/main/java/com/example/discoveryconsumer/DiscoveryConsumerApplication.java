package com.example.discoveryconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
// 开启服务注册功能
@EnableDiscoveryClient
public class DiscoveryConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DiscoveryConsumerApplication.class, args);
    }

}
