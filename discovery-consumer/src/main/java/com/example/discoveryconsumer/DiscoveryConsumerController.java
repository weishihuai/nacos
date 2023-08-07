package com.example.discoveryconsumer;

import com.example.feign.DiscoveryProviderClient;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/consumer")
public class DiscoveryConsumerController {

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private DiscoveryProviderClient discoveryProviderClient;

    @GetMapping(value = "/hello/{text}")
    public String hello(@PathVariable(value = "text") String text) {
        return restTemplate.getForObject("http://discovery-provider/provider/hello/" + text, String.class);
    }

    @GetMapping(value = "/feign/{text}")
    public String feign(@PathVariable(value = "text") String text) {
        return discoveryProviderClient.feign(text);
    }

    /**
     * <p>
     *
     * @HystrixCommand: 将一个方法标记为Hystrix命令，并为该方法提供熔断、隔离、降级等功能。
     * commandKey: 指定Hystrix命令的名称，默认值是方法名
     * fallbackMethod: 指定降级处理的方法名。当Hystrix命令执行失败或超时时，会调用指定的降级方法来返回一个默认值或执行其他的降级逻辑。降级方法的返回值类型和参数列表必须与原始方法相同或兼容。
     * groupKey: 用于指定Hystrix命令的分组名称，默认值是类名。在Hystrix仪表盘中，命令会按照分组进行统计和展示，可以根据不同的业务场景使用不同的分组名称。
     * threadPoolKey: 用于指定Hystrix命令所属的线程池名称，默认值是null，表示使用默认线程池。通过threadPoolKey可以将不同的Hystrix命令分配到不同的线程池中，以实现更精细的线程池控制。
     * ignoreExceptions: 用于指定在执行Hystrix命令时忽略的异常列表，默认为空。当方法抛出指定的异常时，不会触发熔断器，而是直接抛出异常。可以通过这个属性来控制哪些异常需要被忽略，不进行熔断处理。
     * commandProperties: 用于设置Hystrix命令的一些属性，比如执行超时时间、熔断器相关配置等。可以通过@HystrixProperty注解来设置具体的属性值。
     * threadPoolProperties: 用于设置线程池的相关配置，包括coreSize（核心线程数）和maxQueueSize（队列大小）
     * </p>
     */
    @HystrixCommand(fallbackMethod = "hystrixByThreadPoolFallback",
            commandProperties = {
                    @HystrixProperty(name = "execution.isolation.thread.timeoutInMilliseconds", value = "3000")
            },
            commandKey = "hystrix_thread_pool_cmd",
            groupKey = "hystrix_thread_pool_key",
            threadPoolKey = "hystrix_thread_pool_key",
            threadPoolProperties = {
                    @HystrixProperty(name = "coreSize", value = "20"),
                    @HystrixProperty(name = "maxQueueSize", value = "50")
            })
    @GetMapping(value = "/hystrix/threadPool")
    public String hystrixByThreadPool() {
        return restTemplate.getForObject("http://discovery-provider/provider/hystrix", String.class);
    }

    /**
     * 回调方法
     */
    public String hystrixByThreadPoolFallback(Throwable e) {
        return "this is fallback result...";
    }
}
