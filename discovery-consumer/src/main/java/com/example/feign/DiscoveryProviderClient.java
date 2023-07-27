package com.example.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * @Description:
 * @Author: weishihuai
 * @Date: 2023/7/27 11:49
 *
 * <p>
 * 当应用中有多个Feign客户端，并且它们分别调用不同的微服务接口时，如果不正确地配置Feign客户端的name和contextId属性，就可能会导致以下问题：
 *
 * 1、调用错误的微服务接口：如果多个Feign客户端的name属性设置为相同的微服务名称，而没有正确设置contextId属性，就会导致Feign客户端调用错误的微服务接口，从而出现调用失败或错误的结果。
 *
 * 2、重复的Bean定义：在Spring容器中，Feign客户端会被注册为一个Bean。如果多个Feign客户端没有设置contextId属性，并且name属性相同，就会导致重复的Bean定义，从而可能出现Bean冲突或覆盖的问题。
 *
 * 3、Feign客户端的覆盖：如果多个Feign客户端的contextId属性设置相同，那么后面定义的Feign客户端会覆盖前面定义的同名客户端，从而导致其中一个Feign客户端无法正确注册和使用。
 *
 * 为了避免这些问题，应在定义Feign客户端时，确保每个Feign客户端都有唯一的contextId和name属性，以便正确地区分和使用它们。在使用@FeignClient注解定义Feign客户端时，应仔细考虑并指定这些属性，确保不同的Feign客户端之间不会发生冲突和混淆，从而保证应用的正常运行。
 * </p>
 *
 */
@FeignClient(value = "discovery-provider", contextId = "DiscoveryProviderClient")
public interface DiscoveryProviderClient {

    @RequestMapping(value = "/provider/feign/{text}", method = RequestMethod.GET)
    String feign(@PathVariable(value = "text") String text);
}
