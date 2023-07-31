package com.example.discoveryconsumer;

import feign.Feign;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConditionalOnClass({Feign.class})
@Import({
        DirectUrlFeignBuilder.class
})
public class FeignConfiguration {

    @Bean
    @ConditionalOnProperty(value = "feign.url.v1.enable", havingValue = "true")
    public FeignLocalCallBeanPostProcessor feignLocalCallBeanPostProcessor() {
        return new FeignLocalCallBeanPostProcessor();
    }

}
