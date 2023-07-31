package com.example.discoveryconsumer;

import feign.Client;
import feign.Feign;
import feign.Target;
import org.apache.commons.lang.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;

/**
 * 该类只用来本机调试的时候可以连接k8s的服务
 *
 * <p>
 *    在FeignClientsConfiguration配置类中，有一个默认的实现。 使用@ConditionalOnMissingBean注解标识，如果用户自定义了
 *    Feign.Builder，就使用自定义的，否则使用默认的。
 *    @Bean
 *    @Scope("prototype")
 *    @ConditionalOnMissingBean
 *    public Feign.Builder feignBuilder(Retryer retryer) {
 * 		return Feign.builder().retryer(retryer);
 *    }
 * </p>
 */
//@Component
public class DirectUrlFeignBuilder extends Feign.Builder {

    private static final Logger logger = LoggerFactory.getLogger(DirectUrlFeignBuilder.class);

    private String urlPattern = "-8080.%s.qdama.abc";

    private final String URL = "url";

    @Value("${feign.url.enable:false}")
    private Boolean enable;

    @Value("${feign.url.env:stage01}")
    private String env;

    public DirectUrlFeignBuilder() {
        System.out.println("init DirectUrlFeignBuilder");
    }

    @Override
    public <T> T target(Target<T> target) {

        if (!BooleanUtils.isTrue(enable)) {
            return super.target(target);
        }
        String url = target.url();
        if(!url.equals("http://"+target.name())){
            // 说明有人自己指定了，优先级最高，放行
            return super.target(target);
        }
        String targetUrl = url+String.format(urlPattern,env.trim());
        try {
            Field FiledUrl = target.getClass().getDeclaredField(URL);
            FiledUrl.setAccessible(true);
            FiledUrl.set(target, targetUrl);
            logger.debug("url {} 转化为 {} 进行直连的feign 调用", url, targetUrl);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return super.target(target);
    }

    @Override
    public Feign build() {
        if (!BooleanUtils.isTrue(enable)) {
            return super.build();
        }
        // 改成默认的client  ,不再通过注册中心获取服务的ip进行访问
        Client client = new Client.Default(null, null);
        super.client(client);
        return super.build();
    }

    public Boolean getEnable() {
        return enable;
    }

    public void setEnable(Boolean enable) {
        this.enable = enable;
    }
}
