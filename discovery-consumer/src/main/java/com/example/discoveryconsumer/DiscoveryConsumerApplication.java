package com.example.discoveryconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.hystrix.EnableHystrix;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
// 开启服务注册功能
@EnableDiscoveryClient
// 开启feign功能
@EnableFeignClients(basePackages = {"com.example.feign"})
// 开启Hystrix的熔断器功能
@EnableHystrix
public class DiscoveryConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DiscoveryConsumerApplication.class, args);
    }

}
